package com.routex.matching;

@Service
public class DriverAssignmentService {
    private final List<Driver> availableDrivers = List.of(
            new Driver("D101", "Arjun", "KA-01-AB-4200"),
            new Driver("D102", "Rahul", "KA-03-MN-7812"),
            new Driver("D103", "Kiran", "KA-05-ZX-1934")
    );
    public DriverAssignedEvent assignDriver(RideRequestEvent ride){
        Driver driver = availableDrivers.get(
                ThreadLocalRandom.current()
                        .nextInt(availableDrivers.size())
        );
        return new DriverAssignedEvent(
                ride.rideId(),
                ride.passengerId(),
                driver.driverId(),
                driver.driverName(),
                driver.vehicleNumber(),
                Instant.now()
        );
    }
}