package com.routex.matching;

import com.routex.dispatch.RideRequestedEvent;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Service;

@Service
public class DriverAssignmentService {
    private final List<Driver> availableDrivers = List.of(
            new Driver("D101", "Arjun", "KA-01-AB-4200"),
            new Driver("D102", "Rahul", "KA-03-MN-7812"),
            new Driver("D103", "Kiran", "KA-05-ZX-1934")
    );
    public DriverAssignmentEvent assignDriver(RideRequestedEvent ride) {
        Driver driver = availableDrivers.get(
                ThreadLocalRandom.current()
                        .nextInt(availableDrivers.size())
        );
        return new DriverAssignmentEvent(
                ride.rideId(),
                ride.passengerId(),
                driver.driverId(),
                driver.driverName(),
                driver.vehicleNumber(),
                Instant.now()
        );
    }
}
