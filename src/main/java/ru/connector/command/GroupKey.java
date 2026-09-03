package ru.connector.command;

import java.util.Objects;

public record GroupKey(
        MarketType market,
        Type type
) {
    public GroupKey {
        Objects.requireNonNull(market, "MarketType cannot be null in GroupKey");
        Objects.requireNonNull(type, "Type cannot be null in GroupKey");
    }

    public static GroupKey of(MarketType market, Type type) {
        return new GroupKey(market, type);
    }
}
