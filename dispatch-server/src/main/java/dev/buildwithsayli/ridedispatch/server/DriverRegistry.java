package dev.buildwithsayli.ridedispatch.server;

import dev.buildwithsayli.ridedispatch.grpc.Location;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory driver fleet: current location + availability. A real
 * system would back this with a geo-indexed store (e.g. Redis GEO
 * commands or PostGIS) so nearest-match doesn't scan every driver -
 * that's a straightforward Athena extraction point once fleet size
 * makes the O(n) scan below matter.
 */
@Component
public class DriverRegistry {

    private record DriverState(Location location, boolean available) {
    }

    private final Map<String, DriverState> drivers = new ConcurrentHashMap<>();

    @PostConstruct
    void seedDemoDrivers() {
        // A handful of drivers scattered around a fake city center, so
        // there's always at least one available driver for the demo flow.
        drivers.put("driver-1", new DriverState(loc(37.7749, -122.4194), true));
        drivers.put("driver-2", new DriverState(loc(37.7849, -122.4094), true));
        drivers.put("driver-3", new DriverState(loc(37.7649, -122.4294), true));
    }

    private static Location loc(double lat, double lon) {
        return Location.newBuilder().setLatitude(lat).setLongitude(lon).build();
    }

    /**
     * Linear scan for the closest available driver. Fine at 3 drivers;
     * would not be fine at 30,000 - see the class-level note.
     */
    public Optional<Map.Entry<String, Location>> findNearestAvailable(Location pickup) {
        return drivers.entrySet().stream()
                .filter(e -> e.getValue().available())
                .min(Comparator.comparingDouble(e -> GeoUtils.haversineKm(pickup, e.getValue().location())))
                .map(e -> Map.entry(e.getKey(), e.getValue().location()));
    }

    public void markUnavailable(String driverId) {
        drivers.computeIfPresent(driverId, (id, state) -> new DriverState(state.location(), false));
    }

    public void markAvailable(String driverId) {
        drivers.computeIfPresent(driverId, (id, state) -> new DriverState(state.location(), true));
    }

    public void updateLocation(String driverId, Location location) {
        drivers.computeIfPresent(driverId, (id, state) -> new DriverState(location, state.available()));
    }
}
