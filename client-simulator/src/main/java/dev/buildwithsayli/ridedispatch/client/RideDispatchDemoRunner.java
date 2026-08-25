package dev.buildwithsayli.ridedispatch.client;

import dev.buildwithsayli.ridedispatch.grpc.*;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A guided walkthrough of all 4 RPC types against dispatch-server, run
 * as a CommandLineRunner so `mvn spring-boot:run` on this module IS the
 * demo - no separate test harness needed to see the whole contract
 * exercised end to end.
 */
@Component
public class RideDispatchDemoRunner implements CommandLineRunner {

    @GrpcClient("dispatch-server")
    private RideDispatchServiceGrpc.RideDispatchServiceBlockingStub blockingStub;

    @GrpcClient("dispatch-server")
    private RideDispatchServiceGrpc.RideDispatchServiceStub asyncStub;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("\n=== 1. Unary: RequestRide ===");
        RideAssignment assignment = requestRide("rider-42",
                loc(37.7749, -122.4194), loc(37.8044, -122.2712));
        System.out.printf("Matched driver %s, ETA %ds, ride_id=%s%n",
                assignment.getDriverId(), assignment.getEtaSeconds(), assignment.getRideId());

        System.out.println("\n=== 2. Server streaming: WatchRideStatus ===");
        watchRideStatus(assignment.getRideId());

        System.out.println("\n=== 3. Client streaming: StreamDriverLocation ===");
        streamDriverLocation(assignment.getDriverId());

        System.out.println("\n=== 4. Bidirectional streaming: Chat ===");
        chatDemo(assignment.getRideId());

        System.out.println("\n=== 5. Deadline propagation ===");
        deadlineDemo();

        System.out.println("\nDemo complete.");
    }

    private RideAssignment requestRide(String riderId, Location pickup, Location dropoff) {
        return blockingStub.requestRide(RideRequest.newBuilder()
                .setRiderId(riderId)
                .setPickup(pickup)
                .setDropoff(dropoff)
                .build());
    }

    private void watchRideStatus(String rideId) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        asyncStub.watchRideStatus(
                WatchRideStatusRequest.newBuilder().setRideId(rideId).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(RideStatusUpdate update) {
                        System.out.printf("  status -> %s%n", update.getStatus());
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.out.println("  stream error: " + t.getMessage());
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("  ride reached a terminal status - stream closed.");
                        done.countDown();
                    }
                });

        // The demo ride progresses over ~12s (see RideStore.STEP_DELAY x 4
        // steps) - wait generously so this doesn't race the server timers.
        if (!done.await(20, TimeUnit.SECONDS)) {
            System.out.println("  (timed out waiting for ride to complete - is dispatch-server running?)");
        }
    }

    private void streamDriverLocation(String driverId) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<DriverLocationPing> pingStream = asyncStub.streamDriverLocation(
                new StreamObserver<>() {
                    @Override
                    public void onNext(DriverLocationSummary summary) {
                        System.out.printf("  summary: %d pings, %.2f km total, %dms%n",
                                summary.getPingsReceived(), summary.getTotalDistanceKm(), summary.getDurationMs());
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.out.println("  stream error: " + t.getMessage());
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });

        double lat = 37.7749;
        double lon = -122.4194;
        for (int i = 0; i < 5; i++) {
            lat += 0.002;
            lon += 0.002;
            pingStream.onNext(DriverLocationPing.newBuilder()
                    .setDriverId(driverId)
                    .setLocation(loc(lat, lon))
                    .setTimestampEpochMs(System.currentTimeMillis())
                    .build());
            Thread.sleep(300);
        }
        pingStream.onCompleted();
        done.await(5, TimeUnit.SECONDS);
    }

    private void chatDemo(String rideId) throws InterruptedException {
        CountDownLatch riderReceived = new CountDownLatch(1);
        CountDownLatch driverReceived = new CountDownLatch(1);

        // Two participants joined to the same ride - this is what makes the
        // relay demo meaningful instead of a stream talking to itself.
        StreamObserver<ChatMessage> driverOut = asyncStub.chat(new StreamObserver<>() {
            @Override
            public void onNext(ChatMessage m) {
                System.out.printf("  [driver received] %s: %s%n", m.getSenderId(), m.getText());
                driverReceived.countDown();
            }

            @Override
            public void onError(Throwable t) { }

            @Override
            public void onCompleted() { }
        });

        StreamObserver<ChatMessage> riderOut = asyncStub.chat(new StreamObserver<>() {
            @Override
            public void onNext(ChatMessage m) {
                System.out.printf("  [rider received] %s: %s%n", m.getSenderId(), m.getText());
                riderReceived.countDown();
            }

            @Override
            public void onError(Throwable t) { }

            @Override
            public void onCompleted() { }
        });


        driverOut.onNext(chatMessage(rideId, "driver-1", "I'm outside whenever you're ready."));
        Thread.sleep(200);
        riderOut.onNext(chatMessage(rideId, "rider-42", "On my way down, be out in 2 min!"));
        if (!driverReceived.await(2, TimeUnit.SECONDS)) {
            System.out.println("  (driver never received the rider's message - relay may not be working)");
        }
        driverOut.onNext(chatMessage(rideId, "driver-1", "No rush, I'll be here."));
        if (!riderReceived.await(2, TimeUnit.SECONDS)) {
            System.out.println("  (rider never received the driver's reply - relay may not be working)");
        }

        riderOut.onCompleted();
        driverOut.onCompleted();
    }

    private void deadlineDemo() {
        try {
            blockingStub.withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                    .requestRide(RideRequest.newBuilder()
                            .setRiderId("SLOW_TEST")   // dispatch-server sleeps 3s for this exact rider_id
                            .setPickup(loc(37.7749, -122.4194))
                            .setDropoff(loc(37.8044, -122.2712))
                            .build());
            System.out.println("  (unexpected: call did not exceed its deadline)");
        } catch (StatusRuntimeException e) {
            System.out.printf("  call failed as expected: %s%n", e.getStatus().getCode());
        }
    }

    private static ChatMessage chatMessage(String rideId, String senderId, String text) {
        return ChatMessage.newBuilder()
                .setRideId(rideId)
                .setSenderId(senderId)
                .setText(text)
                .setTimestampEpochMs(System.currentTimeMillis())
                .build();
    }

    private static Location loc(double lat, double lon) {
        return Location.newBuilder().setLatitude(lat).setLongitude(lon).build();
    }
}
