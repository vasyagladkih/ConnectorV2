package ru.connector.transport;

import ru.connector.models.Action;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;

public class KucoinRegistry {

    public static String getWsUrl(MarketType type) {
        return type == MarketType.SPOT ? "wss://x-push-spot.kucoin.com" : "wss://x-push-futures.kucoin.com";
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
        if (quote.contains("-")) {
            return base + "-" + quote;
        }
        String suffix = quote.endsWith("M") ? "" : "M";
        return base + quote + suffix;
    }
}
