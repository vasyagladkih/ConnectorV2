package ru.connector.transport;

import java.nio.charset.StandardCharsets;

public class ExchangeTopics {

    public static final byte[] KUCOIN_TRADE = "TRADE.KUCOIN.".getBytes(StandardCharsets.UTF_8);
    public static final byte[] OKX_TRADE = "TRADE.OKX.".getBytes(StandardCharsets.UTF_8);
    public static final byte[] GATEIO_TRADE = "TRADE.GATEIO.".getBytes(StandardCharsets.UTF_8);
    public static final byte[] BYBIT_TRADE = "TRADE.BYBIT.".getBytes(StandardCharsets.UTF_8);
}