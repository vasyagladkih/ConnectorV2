package ru.connector.models;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.datacollector.common.model.MarketType;
import ru.datacollector.common.model.StreamKey;
import ru.datacollector.common.model.Symbol;
import ru.datacollector.common.model.Type;

public record SubscriptionDto(
        @NotBlank(message = "Exchange must not be blank")
        String exchange,

        @NotNull(message = "Market type is required")
        MarketType market,

        @NotNull(message = "Symbol is required")
        @Valid
        Symbol symbol,

        @NotNull(message = "Command is required")
        @Valid
        Command command
) {
    public Type type() {
        return switch (command) {
            case Command.Trades _ -> Type.TRADES;
            case Command.BookTicker _ -> Type.BOOK_TICKER;
            case Command.OrderBook _ -> Type.ORDER_BOOK;
        };
    }

    public StreamKey key() {
        return new StreamKey(exchange, market, symbol, type());
    }
}
