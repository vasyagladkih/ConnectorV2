package ru.connector.exchange.impl.kucoin;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.api.dto.Request;
import ru.connector.exchange.AbstractWebsocketManager;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.Action;
import ru.connector.models.Command;
import ru.connector.models.GroupKey;
import ru.connector.models.MarketType;
import ru.connector.transport.ExchangeTopics;
import ru.connector.transport.KucoinRegistry;

import java.time.Duration;

@Component("KUCOIN")
public class KucoinManager extends AbstractWebsocketManager {

    public static final int DEFAULT_MAX_SUBSCRIPTIONS = 100;

    private final ObjectMapper mapper = new ObjectMapper();
    private final int maxSubscriptionsPerConnection;
    private final long sendDelayMs;

    public KucoinManager(WebSocketClient client, KafkaRawDataPublisher publisher) {
        this(client, publisher, DEFAULT_MAX_SUBSCRIPTIONS, Duration.ofSeconds(60), ExchangeConnection.DEFAULT_SEND_DELAY_MS);
    }

    public KucoinManager(WebSocketClient client,
                         KafkaRawDataPublisher publisher,
                         int maxSubscriptionsPerConnection,
                         Duration idleTimeout) {
        this(client, publisher, maxSubscriptionsPerConnection, idleTimeout, ExchangeConnection.DEFAULT_SEND_DELAY_MS);
    }

    @Autowired
    public KucoinManager(WebSocketClient client,
                         KafkaRawDataPublisher publisher,
                         @Value("${exchange.rate-limit.delay-ms:50}") long sendDelayMs) {
        this(client, publisher, DEFAULT_MAX_SUBSCRIPTIONS, Duration.ofSeconds(60), sendDelayMs);
    }

    public KucoinManager(WebSocketClient client,
                         KafkaRawDataPublisher publisher,
                         int maxSubscriptionsPerConnection,
                         Duration idleTimeout,
                         long sendDelayMs) {
        super(client, publisher, idleTimeout);
        this.maxSubscriptionsPerConnection = maxSubscriptionsPerConnection > 0
                ? maxSubscriptionsPerConnection
                : DEFAULT_MAX_SUBSCRIPTIONS;
        this.sendDelayMs = sendDelayMs >= 0 ? sendDelayMs : ExchangeConnection.DEFAULT_SEND_DELAY_MS;
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
                maxSubscriptionsPerConnection,
                sendDelayMs
        );
    }

    @Override
    public String translate(Request request, Action action) {
        try {
            KucoinOutgoingMsg msg = resolveMsg(request, action);
            return mapper.writeValueAsString(msg);
        } catch (Exception e) {
            throw new RuntimeException("Failed to translate KuCoin request", e);
        }
    }

    @Override
    public String translate(Request request) {
        return translate(request, Action.SUBSCRIBE);
    }

    private KucoinOutgoingMsg resolveMsg(Request request, Action action) {
        String id = String.valueOf(System.currentTimeMillis());
        String actionStr = KucoinRegistry.getAction(action);
        String marketType = KucoinRegistry.getMarketType(request.market());
        String symbol = KucoinRegistry.getSymbol(request.symbol(), request.market());
        String channel = KucoinRegistry.getChannel(request.command());

        return switch (request.command()) {
            case Command.Trades _ ->
                    new KucoinOutgoingMsg.KucoinTradeMsg(id, actionStr, channel, marketType, symbol);

            case Command.BookTicker _ ->
                    new KucoinOutgoingMsg.KucoinTickerMsg(id, actionStr, channel, marketType, symbol);

            case Command.OrderBook ob ->
                    new KucoinOutgoingMsg.KucoinOrderBookMsg(
                            id,
                            actionStr,
                            channel,
                            marketType,
                            symbol,
                            String.valueOf(ob.depth()),
                            0
                    );
        };
    }
}