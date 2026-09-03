package ru.connector.transport;

import ru.connector.models.Action;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;
import ru.connector.api.dto.Request;


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

    public static ru.connector.models.StreamKey generateKey(Request request) {
        String channel = getChannel(request.command());
        String symbolStr = getSymbol(request.symbol(), request.market());
        return ru.connector.models.StreamKey.of(channel, request.market(), symbolStr);
    }

    public static String getAction(Action action) {
        return action == Action.SUBSCRIBE ? "SUBSCRIBE" : "UNSUBSCRIBE";
    }

    public static String getMarketType(MarketType market) {
        return market == MarketType.SPOT ? "SPOT" : "FUTURES";
    }

    public static String getChannel(ru.connector.models.Command command) {
        return switch (command) {
            case ru.connector.models.Command.Trades _ -> "trade";
            case ru.connector.models.Command.BookTicker _ -> "ticker";
            case ru.connector.models.Command.OrderBook _ -> "obu";
        };
    }

    public static String getSymbol(Symbol symbol, MarketType market) {
        return market == MarketType.SPOT ?
                symbol.base() + "-" + symbol.quote() : (symbol.base().equals("BTC") ? "XBT" : symbol.base()) + symbol.quote() + "M";
    }
}