package com.routex.dispatch;

import jakarta.validation.constraints.NotBlank;

public record RideRequest(
        @NotBlank String passengerId,
        @NotBlank String pickupLocation,
        @NotBlank String destinationLocation
) {
}

