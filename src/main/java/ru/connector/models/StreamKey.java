package ru.connector.models;

import ru.connector.api.dto.SubscriptionDto;

public record StreamKey(
        MarketType market,
        Symbol symbol,
        Type type
) {
    public static StreamKey from(SubscriptionDto request) {
        return new StreamKey(
                request.market(),
                request.symbol(),
                request.type()
        );
    }
}