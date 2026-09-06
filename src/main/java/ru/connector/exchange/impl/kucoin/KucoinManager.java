package ru.connector.exchange.impl.kucoin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.net.URI;
import java.util.List;
import java.util.Optional;

@Component("KUCOIN")
public class KucoinManager extends AbstractWebsocketManager {

    private final WebSocketClient wsClient;
    private final KafkaRawDataPublisher rawPublisher;
    private final ObjectMapper objectMapper;

    public KucoinManager(
            SubscriptionsRegistry registry,
            WebSocketClient wsClient,
            KafkaRawDataPublisher rawPublisher,
            ObjectMapper objectMapper) {
        super(registry);
        this.wsClient = wsClient;
        this.rawPublisher = rawPublisher;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean exists(StreamKey key) {
        return registry.exists(key);
    }

    @Override
    public Optional<Long> findIdByStreamKey(StreamKey key) {
        return registry.findIdByStreamKey(key);
    }

    @Override
    public boolean contains(Long id) {
        return registry.findRequestById(id).isPresent();
    }

    @Override
    public Optional<SubscriptionDto> findRequestById(Long id) {
        return registry.findRequestById(id);
    }

    @Override
    public Mono<Void> subscribe(Long id, SubscriptionDto sub) {
        return Mono.defer(() -> {
            GroupKey groupKey = GroupKey.of(sub.market(), sub.type());
            ExchangeConnection conn = registry.getOrCreateConnection(groupKey, () -> createConnection(groupKey));
            String frame = translate(sub, Action.SUBSCRIBE);

            conn.acquireSlot();
            return conn.send(frame)
                    .doOnSuccess(_ -> registry.register(id, sub, conn))
                    .doOnError(_ -> conn.releaseSlot());
        });
    }

    @Override
    public Mono<Void> unsubscribe(Long id) {
        return Mono.defer(() -> {
            SubscriptionDto sub = registry.findRequestById(id)
                    .orElseThrow(() -> new SubscriptionNotFoundException(id));
            ExchangeConnection conn = registry.findConnectionById(id)
                    .orElseThrow(() -> new IllegalStateException("Connection not found for subscription id: " + id));

            String frame = translate(sub, Action.UNSUBSCRIBE);

            return conn.send(frame)
                    .doFinally(_ -> {
                        conn.releaseSlot();
                        registry.unregister(id);
                    });
        });
    }

    @Override
    public List<SubscriptionResponse> getAllActive() {
        return registry.getAllActive().stream()
                .map(req -> new SubscriptionResponse(
                        registry.findIdByStreamKey(StreamKey.from(req)).orElse(0L),
                        req.exchange(),
                        req.market(),
                        req.symbol(),
                        req.command()
                ))
                .toList();
    }

    @Override
    public String translate(SubscriptionDto request, Action action) {
        KucoinOutgoingMsg msg = resolveMsg(request, action);
        try {
            return objectMapper.writeValueAsString(msg);
        } catch (JsonProcessingException e) {
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
        String partitionKey = "KUCOIN:" + groupKey.market();
        ExchangeConnection conn = new ExchangeConnection(
                url,
                wsClient,
                bytes -> rawPublisher.publish(partitionKey, bytes)
        );
        conn.start();
        return conn;
    }

    @Override
    public void shutdown() {
        registry.clear();
    }
}