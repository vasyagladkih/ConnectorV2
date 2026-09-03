package ru.connector.command;

import java.util.Objects;

public record SubscriptionKey(
        MarketType market,
        Symbol symbol,
        Command command
) {
    public SubscriptionKey {
        Objects.requireNonNull(market, "MarketType cannot be null in SubscriptionKey");
        Objects.requireNonNull(symbol, "Symbol cannot be null in SubscriptionKey");
        Objects.requireNonNull(command, "Command cannot be null in SubscriptionKey");
    }

    public static SubscriptionKey of(MarketType market, Symbol symbol, Command command) {
        return new SubscriptionKey(market, symbol, command);
    }

    public static SubscriptionKey from(Request request) {
        return new SubscriptionKey(request.market(), request.symbol(), request.command());
    }

    public GroupKey toGroupKey() {
        return new GroupKey(market, Type.type(command));
    }
}
