package ru.connector.transport;

import ru.connector.api.dto.SubscriptionDto;
import ru.connector.models.Action;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.StreamKey;
import ru.connector.models.Symbol;

public class KucoinRegistry {

    public static String getRestUrl(MarketType market) {
        return switch (market) {
            case SPOT -> "https://api.kucoin.com/api/v1/bullet-public";
            case FUTURES -> "https://api-futures.kucoin.com/api/v1/bullet-public";
        };
    }

    public static String getWsUrl(MarketType type) {
        return type == MarketType.SPOT ? "wss://x-push-spot.kucoin.com" : "wss://x-push-futures.kucoin.com";
    }

    public static StreamKey generateKey(SubscriptionDto request) {
        return StreamKey.from(request);
    }

    public static String getAction(Action action) {
        return action == Action.SUBSCRIBE ? "subscribe" : "unsubscribe";
    }

    public static String getMarketType(MarketType market) {
        return market == MarketType.SPOT ? "SPOT" : "FUTURES";
    }

    public static String getChannel(Command command) {
        return switch (command) {
            case Command.Trades _ -> "trade";
            case Command.BookTicker _ -> "ticker";
            case Command.OrderBook _ -> "obu";
        };
    }

    public static String getSymbol(Symbol symbol, MarketType market) {
        if (market == MarketType.SPOT) {
            return symbol.base() + "-" + symbol.quote();
        }
        String base = symbol.base().equalsIgnoreCase("BTC") ? "XBT" : symbol.base();
        String quote = symbol.quote();
        String suffix = quote.endsWith("M") ? "" : "M";
        return base + quote + suffix;
    }


}