package ru.connector.models;

import ru.connector.api.dto.Request;

import java.util.Objects;

public record SubscriptionKey(
        String exchange,
        Type type,
        MarketType market,
        Symbol symbol,
        Command command
) {
    public SubscriptionKey(String exchange, Type type, MarketType market, Symbol symbol) {
        this(exchange, type, market, symbol, null);
    }

    public SubscriptionKey(MarketType market, Symbol symbol, Command command) {
        this(null, command != null ? Type.type(command) : null, market, symbol, command);
    }

    public static SubscriptionKey of(MarketType market, Symbol symbol, Command command) {
        return new SubscriptionKey(market, symbol, command);
    }

    public static SubscriptionKey of(String exchange, Type type, MarketType market, Symbol symbol) {
        return new SubscriptionKey(exchange, type, market, symbol);
    }

    public static SubscriptionKey from(Request request) {
        return new SubscriptionKey(
                request.exchange(),
                request.type(),
                request.market(),
                request.symbol(),
                request.command()
        );
    }

    public GroupKey toGroupKey() {
        return new GroupKey(market, type != null ? type : Type.type(command));
    }

    @Override
    public String toString() {
        if (exchange != null) {
            return exchange + ":" + (type != null ? type : "") + ":" + market + ":" + symbol;
        }
        return market + ":" + symbol + ":" + (command != null ? command : type);
    }
}
