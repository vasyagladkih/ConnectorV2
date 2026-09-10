package ru.connector.exchange.registry;

import ru.connector.api.dto.SubscriptionDto;

public record SubscriptionState(
        SubscriptionDto request,
        Status status
) {
    public enum Status {PENDING, ACTIVE}

    public static SubscriptionState pending(SubscriptionDto request) {
        return new SubscriptionState(request, Status.PENDING);
    }

    public SubscriptionState toActive() {
        return new SubscriptionState(request, Status.ACTIVE);
    }
}
