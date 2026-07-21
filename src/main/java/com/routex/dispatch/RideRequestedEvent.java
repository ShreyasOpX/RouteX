package com.routex.dispatch;

import java.time.Instant;

public record RideRequestedEvent(
        String rideId,
        String passengerId,
        String pickupLocation,
        String destinationLocation,
        Instant requestedAt
) {
}

