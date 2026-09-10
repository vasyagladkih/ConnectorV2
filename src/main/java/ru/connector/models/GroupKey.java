package ru.connector.models;

import org.jspecify.annotations.Nullable;

public record GroupKey(
        MarketType market,
        @Nullable Symbol symbol,
        Type type
) {
    public static GroupKey of(MarketType market, Symbol symbol, Type type) {
        return new GroupKey(market, symbol, type);
    }

    public static GroupKey of(MarketType market, Type type) {
        return new GroupKey(market, null, type);
    }
}
