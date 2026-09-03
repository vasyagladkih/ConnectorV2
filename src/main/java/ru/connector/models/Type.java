package ru.connector.models;


public enum Type {
    TRADES, BOOK_TICKER, ORDER_BOOK;

    public static Type type(Command command) {
        return switch (command) {
           case Command.OrderBook _ -> Type.ORDER_BOOK;
           case Command.Trades _ -> Type.TRADES;
           case Command.BookTicker _ -> Type.BOOK_TICKER;
        };
    }
}