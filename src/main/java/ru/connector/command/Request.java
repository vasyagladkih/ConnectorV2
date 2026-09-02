package ru.connector.command;

public record Request(
        String exchange,
        Action action,
        MarketType market,
        Symbol symbol,
        Command command
) {}