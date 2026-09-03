package ru.connector.exchange.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.command.Command;
import ru.connector.command.GroupKey;
import ru.connector.command.MarketType;
import ru.connector.command.Request;
import ru.connector.exchange.AbstractWebsocketManager;
import ru.connector.exchange.impl.kucoin.KucoinOutgoingMsg;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.transport.ExchangeTopics;
import ru.connector.transport.KafkaPublisher;
import ru.connector.transport.KucoinRegistry;

import java.time.Duration;

public class KucoinManager extends AbstractWebsocketManager {

    public static final int DEFAULT_MAX_SUBSCRIPTIONS = 100;

    private final ObjectMapper mapper = new ObjectMapper();
    private final int maxSubscriptionsPerConnection;

    public KucoinManager(WebSocketClient client, KafkaPublisher publisher) {
        this(client, publisher, DEFAULT_MAX_SUBSCRIPTIONS, Duration.ofSeconds(60));
    }

    public KucoinManager(WebSocketClient client,
                         KafkaPublisher publisher,
                         int maxSubscriptionsPerConnection,
                         Duration idleTimeout) {
        super(client, publisher, idleTimeout);
        this.maxSubscriptionsPerConnection = maxSubscriptionsPerConnection > 0
                ? maxSubscriptionsPerConnection
                : DEFAULT_MAX_SUBSCRIPTIONS;
    }

    @Override
    protected ExchangeConnection createConnection(GroupKey groupKey) {
        MarketType market = groupKey.market();
        return new ExchangeConnection(
                "KUCOIN",
                market,
                KucoinRegistry.getWsUrl(market),
                client,
                publisher,
                new String(ExchangeTopics.KUCOIN_TRADE),
                Duration.ofSeconds(20),
                maxSubscriptionsPerConnection
        );
    }

    @Override
    public String translate(Request request) {
        try {
            KucoinOutgoingMsg msg = resolveMsg(request);
            return mapper.writeValueAsString(msg);
        } catch (Exception e) {
            throw new RuntimeException("Failed to translate KuCoin request", e);
        }
    }

    private KucoinOutgoingMsg resolveMsg(Request request) {
        String id = String.valueOf(System.currentTimeMillis());
        String action = KucoinRegistry.getAction(request.action());
        String marketType = KucoinRegistry.getMarketType(request.market());
        String symbol = KucoinRegistry.getSymbol(request.symbol(), request.market());
        String channel = KucoinRegistry.getChannel(request.command());

        return switch (request.command()) {
            case Command.Trades _ ->
                    new KucoinOutgoingMsg.KucoinTradeMsg(id, action, channel, marketType, symbol);

            case Command.BookTicker _ ->
                    new KucoinOutgoingMsg.KucoinTickerMsg(id, action, channel, marketType, symbol);

            case Command.OrderBook ob ->
                    new KucoinOutgoingMsg.KucoinOrderBookMsg(
                            id,
                            action,
                            channel,
                            marketType,
                            symbol,
                            String.valueOf(ob.depth()),
                            0
                    );
        };
    }
}