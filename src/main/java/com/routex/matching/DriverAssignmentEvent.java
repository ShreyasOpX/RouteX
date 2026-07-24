package com.routex.matching;
import java.time.Instant;
public record DriverAssignmentEvent(
        String rideId,
        String passengerId,
        String driverId,
        String driverName,
        String vehicleNumber,
        Instant assignedAt
) {}
