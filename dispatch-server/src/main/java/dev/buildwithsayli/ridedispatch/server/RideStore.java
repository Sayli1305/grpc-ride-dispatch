package dev.buildwithsayli.ridedispatch.server;

import dev.buildwithsayli.ridedispatch.grpc.Location;
import dev.buildwithsayli.ridedispatch.grpc.RideStatus;
import dev.buildwithsayli.ridedispatch.grpc.RideStatusUpdate;
import io.grpc.stub.StreamObserver;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Ride state machine + fan-out to whoever is watching it via
 * WatchRideStatus. A ride, once matched, progresses through a fixed
 * sequence of statuses on a timer - this is a SIMULATION standing in
 * for what would really be driven by actual driver telemetry and
 * dispatch events in production. It exists so the server-streaming RPC
 * has real state changes to push, not just one message.
 */
@Component
public class RideStore {

    private static final List<RideStatus> PROGRESSION = List.of(
            RideStatus.DRIVER_EN_ROUTE, RideStatus.DRIVER_ARRIVED, RideStatus.IN_PROGRESS, RideStatus.COMPLETED
    );
    private static final Duration STEP_DELAY = Duration.ofSeconds(3);

    private static final class RideState {
        final String riderId;
        final String driverId;
        volatile RideStatus status = RideStatus.MATCHED;
        volatile Location currentLocation;
        final List<StreamObserver<RideStatusUpdate>> subscribers = new CopyOnWriteArrayList<>();

        RideState(String riderId, String driverId, Location currentLocation) {
            this.riderId = riderId;
            this.driverId = driverId;
            this.currentLocation = currentLocation;
        }
    }

    private final Map<String, RideState> rides = new ConcurrentHashMap<>();
    private final TaskScheduler taskScheduler;
    private final DriverRegistry driverRegistry;

    public RideStore(TaskScheduler taskScheduler, DriverRegistry driverRegistry) {
        this.taskScheduler = taskScheduler;
        this.driverRegistry = driverRegistry;
    }

    public void createRide(String rideId, String riderId, String driverId, Location driverLocation) {
        RideState state = new RideState(riderId, driverId, driverLocation);
        rides.put(rideId, state);
        scheduleProgression(rideId, 0);
    }

    private void scheduleProgression(String rideId, int stepIndex) {
        if (stepIndex >= PROGRESSION.size()) {
            return;
        }
        taskScheduler.schedule(() -> {
            RideStatus next = PROGRESSION.get(stepIndex);
            advance(rideId, next);
            scheduleProgression(rideId, stepIndex + 1);
        }, Instant.now().plus(STEP_DELAY));
    }

    private void advance(String rideId, RideStatus status) {
        RideState state = rides.get(rideId);
        if (state == null) {
            return; // ride was already cleaned up - nothing to push to
        }
        state.status = status;

        RideStatusUpdate update = RideStatusUpdate.newBuilder()
                .setRideId(rideId)
                .setStatus(status)
                .setCurrentLocation(state.currentLocation)
                .setTimestampEpochMs(System.currentTimeMillis())
                .build();

        boolean terminal = status == RideStatus.COMPLETED || status == RideStatus.CANCELLED;
        for (StreamObserver<RideStatusUpdate> subscriber : state.subscribers) {
            try {
                subscriber.onNext(update);
                if (terminal) {
                    subscriber.onCompleted();
                }
            } catch (Exception e) {
                // A subscriber's stream is already dead (client disconnected
                // without cancelling cleanly) - don't let that break the
                // broadcast to everyone else watching the same ride.
                state.subscribers.remove(subscriber);
            }
        }

        if (terminal) {
            driverRegistry.markAvailable(state.driverId);
            rides.remove(rideId);
        }
    }

    /**
     * Registers a watcher and immediately pushes the CURRENT status, not
     * just future ones - a client that subscribes after MATCHED already
     * happened should still see where the ride actually is right now.
     */
    public boolean subscribe(String rideId, StreamObserver<RideStatusUpdate> observer) {
        RideState state = rides.get(rideId);
        if (state == null) {
            return false;
        }
        state.subscribers.add(observer);
        observer.onNext(RideStatusUpdate.newBuilder()
                .setRideId(rideId)
                .setStatus(state.status)
                .setCurrentLocation(state.currentLocation)
                .setTimestampEpochMs(System.currentTimeMillis())
                .build());
        return true;
    }

    /**
     * Called from the server call's cancellation handler when a client
     * disconnects mid-stream - without this, a churn of short-lived
     * watchers would leak StreamObservers indefinitely.
     */
    public void unsubscribe(String rideId, StreamObserver<RideStatusUpdate> observer) {
        Optional.ofNullable(rides.get(rideId)).ifPresent(state -> state.subscribers.remove(observer));
    }

    public boolean exists(String rideId) {
        return rides.containsKey(rideId);
    }
}
