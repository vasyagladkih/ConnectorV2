package ru.connector.api.dto;

import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;

public record SubscriptionResponse(
        String id,
        String exchange,
        MarketType market,
        Symbol symbol,
        Command command
) {

    public static SubscriptionResponse toResponse(String id, SubscriptionDto request) {
        return new SubscriptionResponse(
                id,
                request.exchange(),
                request.market(),
                request.symbol(),
                request.command()
        );
    }
}