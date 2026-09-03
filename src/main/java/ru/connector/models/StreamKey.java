package ru.connector.models;

public record StreamKey(
        String channel,
        MarketType market,
        String symbol
) {
    public static StreamKey of(String channel, MarketType market, String symbol) {
        return new StreamKey(channel, market, symbol);
    }
}