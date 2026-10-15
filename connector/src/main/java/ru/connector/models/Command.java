package ru.connector.models;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Command.Trades.class, name = "trades"),
        @JsonSubTypes.Type(value = Command.BookTicker.class, name = "book_ticker"),
        @JsonSubTypes.Type(value = Command.BookTicker.class, name = "ticker"),
        @JsonSubTypes.Type(value = Command.OrderBook.class, name = "orderbook")
})
public sealed interface Command permits
        Command.Trades,
        Command.BookTicker,
        Command.OrderBook {

    record Trades() implements Command {}
    // TODO: временно отключено, текущий фокус только на TRADES
    record BookTicker() implements Command {}
    // TODO: временно отключено, текущий фокус только на TRADES
    record OrderBook(int depth) implements Command {}
}