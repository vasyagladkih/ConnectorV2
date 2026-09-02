package ru.connector.exchange.impl.kucoin;

public sealed interface KucoinOutgoingMsg permits
        KucoinOutgoingMsg.KucoinTradeMsg,
        KucoinOutgoingMsg.KucoinTickerMsg,
        KucoinOutgoingMsg.KucoinOrderBookMsg {

    record KucoinTickerMsg(
            String id,
            String action,
            String channel,
            String tradeType,
            String symbol
    ) implements KucoinOutgoingMsg {}

    record KucoinTradeMsg(
            String id,
            String action,
            String channel,
            String tradeType,
            String symbol
    ) implements KucoinOutgoingMsg {}

    record KucoinOrderBookMsg(
            String id,
            String action,
            String channel,
            String tradeType,
            String symbol,
            String depth,
            int rpiFilter
    ) implements KucoinOutgoingMsg {}
}