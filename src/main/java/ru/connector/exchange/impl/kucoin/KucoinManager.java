package ru.connector.exchange.impl.kucoin;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.AbstractWebsocketManager;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.exchange.registry.SubscriptionsRegistry;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.Action;
import ru.connector.models.Command;
import ru.connector.models.GroupKey;
import ru.connector.models.StreamKey;
import ru.connector.transport.KucoinRegistry;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

@Component("KUCOIN")
public class KucoinManager extends AbstractWebsocketManager {

    private final WebSocketClient wsClient;
    private final KafkaRawDataPublisher rawPublisher;
    private final JsonMapper jsonMapper;

    public KucoinManager(
            SubscriptionsRegistry registry,
            WebSocketClient wsClient,
            KafkaRawDataPublisher rawPublisher,
            JsonMapper jsonMapper) {
        super(registry);
        this.wsClient = wsClient;
        this.rawPublisher = rawPublisher;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public boolean tryReserve(StreamKey key, SubscriptionDto request) {
        return registry.tryReserve(key, request);
    }

    @Override
    public boolean exists(StreamKey key) {
        return registry.exists(key);
    }

    @Override
    public void rollback(StreamKey key) {
        registry.rollback(key);
    }

    @Override
    public Mono<Void> subscribe(StreamKey key, SubscriptionDto sub) {
        GroupKey groupKey = GroupKey.of(sub.market(), sub.type());
        ExchangeConnection conn = registry.getOrCreateConnection(groupKey, () -> createConnection(groupKey));
        String frame = translate(sub, Action.SUBSCRIBE);

        conn.acquireSlot(key);
        return conn.send(frame)
                .doOnSuccess(_ -> registry.activate(key, conn))
                .doOnError(_ -> {
                    conn.releaseSlot(key);
                    registry.rollback(key);
                })
                .doOnCancel(() -> {
                    conn.releaseSlot(key);
                    registry.rollback(key);
                });
    }

    @Override
    public Mono<Void> unsubscribe(StreamKey key) {
        Optional<SubscriptionDto> subOpt = registry.findRequestByKey(key);
        if (subOpt.isEmpty()) {
            return Mono.error(new SubscriptionNotFoundException("Subscription not found for stream: " + key));
        }
        SubscriptionDto sub = subOpt.get();

        Optional<ExchangeConnection> connOpt = registry.findConnectionByKey(key);
        if (connOpt.isEmpty()) {
            registry.rollback(key);
            return Mono.empty();
        }

        ExchangeConnection conn = connOpt.get();
        String frame = translate(sub, Action.UNSUBSCRIBE);

        return conn.send(frame)
                .doFinally(_ -> {
                    if (registry.unregister(key)) {
                        conn.releaseSlot(key);
                    }
                });
    }

    @Override
    public List<SubscriptionResponse> getAllActive() {
        return registry.getAllActive().stream()
                .map(SubscriptionResponse::toResponse)
                .toList();
    }

    @Override
    public String translate(SubscriptionDto request, Action action) {
        KucoinOutgoingMsg msg = resolveMsg(request, action);
        try {
            return jsonMapper.writeValueAsString(msg);
        } catch (JacksonException e) {
            throw new RuntimeException("Failed to serialize Kucoin message: " + msg, e);
        }
    }

    private KucoinOutgoingMsg resolveMsg(SubscriptionDto request, Action action) {
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

    private ExchangeConnection createConnection(GroupKey groupKey) {
        URI url = URI.create(KucoinRegistry.getWsUrl(groupKey.market()));
        ExchangeConnection conn = new ExchangeConnection(
                url,
                wsClient,
                bytes -> {
                    String symbolStr = groupKey.symbol() != null ? groupKey.symbol().toString() : extractSymbolFromPayload(bytes);
                    String partitionKey = "KUCOIN:" + groupKey.market() + (symbolStr != null ? ":" + symbolStr : "");
                    rawPublisher.publish(partitionKey, bytes);
                }
        );
        conn.start();
        return conn;
    }

    private String extractSymbolFromPayload(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        int sIdx = text.indexOf("\"s\":\"");
        if (sIdx != -1) {
            int start = sIdx + 5;
            int end = text.indexOf("\"", start);
            if (end != -1) {
                return text.substring(start, end);
            }
        }
        int tIdx = text.indexOf("\"T\":\"trade.");
        if (tIdx != -1) {
            int start = tIdx + 11;
            int end = text.indexOf("\"", start);
            if (end != -1) {
                return text.substring(start, end);
            }
        }
        return null;
    }

    @Override
    public void shutdown() {
        registry.clear();
    }
}
