package ru.connector.api.dto;

import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;

public record SubscriptionResponse(
        Long id,
        String exchange,
        MarketType market,
        Symbol symbol,
        Command command
) {}
