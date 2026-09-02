package ru.connector.exchange.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.command.*;
import ru.connector.command.Request;
import ru.connector.exchange.AbstractWebsocketManager;
import ru.connector.exchange.impl.kucoin.*;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.transport.ExchangeTopics;
import ru.connector.transport.KafkaPublisher;
import ru.connector.transport.KucoinRegistry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static ru.connector.command.Type.*;

@Service
public class KucoinManager extends AbstractWebsocketManager {

    private final ObjectMapper mapper = new ObjectMapper();

    public KucoinManager(WebSocketClient client, KafkaPublisher publisher) {
        super(client, publisher);
    }

    @Override
    public void subscribe(Request request) {
        ExchangeConnection conn = getOrCreateConnection(request);
        conn.subscribe(KucoinRegistry.generateKey(request), translate(request));
    }

    @Override
    public void unsubscribe(Request request) {
        getConnectionIfPresent(request).ifPresent(conn ->
                conn.unsubscribe(KucoinRegistry.generateKey(request), translate(request)));
    }

    private ExchangeConnection getOrCreateConnection(Request request) {
        MarketType market = request.market();
        Type streamType = type(request.command());

        return connections
                .computeIfAbsent(market, _ -> new ConcurrentHashMap<>())
                .computeIfAbsent(streamType, _ -> createConnection(request));
    }

    private Optional<ExchangeConnection> getConnectionIfPresent(Request request) {
        MarketType market = request.market();
        Type streamType = type(request.command());

        Map<Type, ExchangeConnection> marketMap = connections.get(market);
        if (marketMap == null) return Optional.empty();

        return Optional.ofNullable(marketMap.get(streamType));
    }

    private ExchangeConnection createConnection(Request request) {
        var market = request.market();

        ExchangeConnection connection = new ExchangeConnection(
                "KUCOIN",
                market,
                KucoinRegistry.getWsUrl(market),
                client,
                publisher,
                exchangeTopicsPrefix(request.command()),
                java.time.Duration.ofSeconds(20)
        );
        connection.start();
        return connection;
    }

    /**
     * Префикс Kafka-ключа для партиционирования. По аналогии с ExchangeTopics
     * исходного коннектора, все сообщения биржи идут под одним префиксом-ключом.
     */
    private String exchangeTopicsPrefix(Command command) {
        return new String(ExchangeTopics.KUCOIN_TRADE);
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