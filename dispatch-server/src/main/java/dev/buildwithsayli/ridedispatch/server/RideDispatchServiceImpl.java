package dev.buildwithsayli.ridedispatch.server;

import dev.buildwithsayli.ridedispatch.grpc.*;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * All four RPCs live here because they share so much state (DriverRegistry,
 * RideStore, ChatRelay) that splitting them into separate classes would
 * mean passing the same three collaborators around for no real benefit -
 * this is the one class where "just implement the generated interface" is
 * the right amount of structure.
 */
@GrpcService
public class RideDispatchServiceImpl extends RideDispatchServiceGrpc.RideDispatchServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RideDispatchServiceImpl.class);
    private static final double ASSUMED_SPEED_KMH = 30.0;

    private final DriverRegistry driverRegistry;
    private final RideStore rideStore;
    private final ChatRelay chatRelay;

    public RideDispatchServiceImpl(DriverRegistry driverRegistry, RideStore rideStore, ChatRelay chatRelay) {
        this.driverRegistry = driverRegistry;
        this.rideStore = rideStore;
        this.chatRelay = chatRelay;
    }

    // ------------------------------------------------------------------
    // Unary: RequestRide
    // ------------------------------------------------------------------
    @Override
    public void requestRide(RideRequest request, StreamObserver<RideAssignment> responseObserver) {
        try {
            if (request.getRiderId().isBlank()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("rider_id is required").asRuntimeException());
                return;
            }

            // Deliberate, clearly-marked hook for demonstrating deadline
            // propagation from the client simulator: a normal request never
            // hits this path. Real slowness (an overloaded matching
            // algorithm, a slow geo-index lookup) is what a deadline is
            // actually protecting against in production.
            if ("SLOW_TEST".equals(request.getRiderId())) {
                Thread.sleep(3000);
            }

            Optional<Map.Entry<String, Location>> nearest = driverRegistry.findNearestAvailable(request.getPickup());
            if (nearest.isEmpty()) {
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("no drivers available").asRuntimeException());
                return;
            }

            String driverId = nearest.get().getKey();
            Location driverLocation = nearest.get().getValue();
            String rideId = UUID.randomUUID().toString();

            driverRegistry.markUnavailable(driverId);
            rideStore.createRide(rideId, request.getRiderId(), driverId, driverLocation);

            double distanceKm = GeoUtils.haversineKm(driverLocation, request.getPickup());
            int etaSeconds = (int) Math.round((distanceKm / ASSUMED_SPEED_KMH) * 3600);

            responseObserver.onNext(RideAssignment.newBuilder()
                    .setRideId(rideId)
                    .setDriverId(driverId)
                    .setDriverCurrentLocation(driverLocation)
                    .setEtaSeconds(etaSeconds)
                    .build());
            responseObserver.onCompleted();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            responseObserver.onError(Status.CANCELLED.withCause(e).asRuntimeException());
        } catch (Exception e) {
            // Never let an unexpected exception surface as bare UNKNOWN -
            // log it server-side and give the client an actionable status.
            log.error("requestRide failed", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("internal error matching a driver").withCause(e).asRuntimeException());
        }
    }

    // ------------------------------------------------------------------
    // Server streaming: WatchRideStatus
    // ------------------------------------------------------------------
    @Override
    public void watchRideStatus(WatchRideStatusRequest request, StreamObserver<RideStatusUpdate> responseObserver) {
        String rideId = request.getRideId();

        if (!rideStore.exists(rideId)) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("no such ride: " + rideId).asRuntimeException());
            return;
        }

        // Without this, a rider who closes the app mid-ride would leave
        // this server pushing updates into the void forever.
        if (responseObserver instanceof ServerCallStreamObserver<?> serverObserver) {
            serverObserver.setOnCancelHandler(() -> rideStore.unsubscribe(rideId, responseObserver));
        }

        rideStore.subscribe(rideId, responseObserver);
    }

    // ------------------------------------------------------------------
    // Client streaming: StreamDriverLocation
    // ------------------------------------------------------------------
    @Override
    public StreamObserver<DriverLocationPing> streamDriverLocation(StreamObserver<DriverLocationSummary> responseObserver) {
        return new StreamObserver<>() {
            private String driverId;
            private Location lastLocation;
            private int pingCount = 0;
            private double totalDistanceKm = 0.0;
            private final long startTimeMs = System.currentTimeMillis();

            @Override
            public void onNext(DriverLocationPing ping) {
                driverId = ping.getDriverId();
                pingCount++;
                if (lastLocation != null) {
                    totalDistanceKm += GeoUtils.haversineKm(lastLocation, ping.getLocation());
                }
                lastLocation = ping.getLocation();
                driverRegistry.updateLocation(driverId, ping.getLocation());
            }

            @Override
            public void onError(Throwable t) {
                // The driver's app likely crashed or lost connectivity
                // mid-stream - log and move on rather than propagating,
                // since there's no client left listening for a response.
                log.warn("driver location stream for {} ended with error: {}", driverId, t.toString());
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(DriverLocationSummary.newBuilder()
                        .setDriverId(driverId == null ? "" : driverId)
                        .setPingsReceived(pingCount)
                        .setTotalDistanceKm(totalDistanceKm)
                        .setDurationMs(System.currentTimeMillis() - startTimeMs)
                        .build());
                responseObserver.onCompleted();
            }
        };
    }

    // ------------------------------------------------------------------
    // Bidirectional streaming: Chat
    // ------------------------------------------------------------------
    @Override
    public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver) {
        return new StreamObserver<>() {
            private String joinedRideId;

            @Override
            public void onNext(ChatMessage message) {
                if (joinedRideId == null) {
                    joinedRideId = message.getRideId();
                    chatRelay.join(joinedRideId, responseObserver);
                }
                chatRelay.broadcast(joinedRideId, message, responseObserver);
            }

            @Override
            public void onError(Throwable t) {
                log.warn("chat stream for ride {} ended with error: {}", joinedRideId, t.toString());
                chatRelay.leave(joinedRideId, responseObserver);
            }

            @Override
            public void onCompleted() {
                chatRelay.leave(joinedRideId, responseObserver);
                responseObserver.onCompleted();
            }
        };
    }
}
