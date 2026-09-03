package ru.connector.models;

import java.time.Instant;

public record ClientSubscription(
        Long id,
        String exchange,
        SubscriptionKey key,
        Command command,
        Instant createdAt
) {}
