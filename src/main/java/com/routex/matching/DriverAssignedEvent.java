package com.routex.matching;
import java.time.Instant;
public record DriverAssignedEvent(
        String rideId;
        String passengerId;
        String driverId;
        String driverName;
        String vehicleNumber;
        Instant addignedAt;
) {}