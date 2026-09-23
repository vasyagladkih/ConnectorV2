package ru.connector.exchange.impl.kucoin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.AbstractWebsocketManager;
import ru.connector.exchange.actor.GroupPoolActor;
import ru.connector.exchange.network.ExchangeAdapter;
import ru.connector.exchange.registry.SubscriptionsRegistry;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.Action;
import ru.connector.models.Command;
import ru.connector.models.GroupKey;
import ru.connector.models.StreamKey;
import ru.connector.transport.KucoinRegistry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Менеджер подключения к бирже KuCoin на базе акторной модели.
 * <p>
 * Делегирует маршрутизацию подписок и управление пулом сокетов через {@link GroupPoolActor}.
 */
@Component("KUCOIN")
public class KucoinManager extends AbstractWebsocketManager {

    private final WebSocketClient wsClient;
    private final KafkaRawDataPublisher rawPublisher;
    private final ObjectMapper objectMapper;
    private final ExchangeAdapter adapter;

    private final Map<GroupKey, GroupPoolActor> pools = new ConcurrentHashMap<>();
    private final AtomicLong idSequence = new AtomicLong(System.currentTimeMillis());

    public KucoinManager(
            WebSocketClient wsClient,
            KafkaRawDataPublisher rawPublisher,
            ObjectMapper objectMapper,
            ExchangeAdapter adapter
    ) {
        this(null, wsClient, rawPublisher, objectMapper, adapter);
    }

    public KucoinManager(
            @Nullable SubscriptionsRegistry registry,
            WebSocketClient wsClient,
            KafkaRawDataPublisher rawPublisher,
            ObjectMapper objectMapper
    ) {
        this(registry, wsClient, rawPublisher, objectMapper, new KucoinAdapter());
    }

    @Autowired
    public KucoinManager(
            @Nullable SubscriptionsRegistry registry,
            WebSocketClient wsClient,
            KafkaRawDataPublisher rawPublisher,
            ObjectMapper objectMapper,
            ExchangeAdapter adapter
    ) {
        super(registry);
        this.wsClient = wsClient;
        this.rawPublisher = rawPublisher;
        this.objectMapper = objectMapper;
        this.adapter = adapter;
    }

    @Override
    public boolean exists(StreamKey key) {
        for (GroupPoolActor pool : pools.values()) {
            if (pool.exists(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<Long> findIdByStreamKey(StreamKey key) {
        for (GroupPoolActor pool : pools.values()) {
            Optional<Long> found = pool.findIdByStreamKey(key);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean contains(Long id) {
        for (GroupPoolActor pool : pools.values()) {
            if (pool.contains(id)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<SubscriptionDto> findRequestById(Long id) {
        for (GroupPoolActor pool : pools.values()) {
            Optional<SubscriptionDto> found = pool.findRequestById(id);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    @Override
    public Mono<Void> subscribe(Long id, SubscriptionDto sub) {
        return Mono.defer(() -> {
            GroupKey groupKey = GroupKey.of(sub.market(), sub.type());
            GroupPoolActor poolActor = getOrCreatePool(groupKey);
            return poolActor.subscribe(id, sub);
        });
    }

    @Override
    public Mono<Void> unsubscribe(Long id) {
        return Mono.defer(() -> {
            for (GroupPoolActor pool : pools.values()) {
                if (pool.contains(id)) {
                    return pool.unsubscribe(id);
                }
            }
            return Mono.error(new SubscriptionNotFoundException(id));
        });
    }

    @Override
    public List<SubscriptionResponse> getAllActive() {
        return pools.values().stream()
                .flatMap(pool -> pool.getAllActive().stream())
                .map(req -> new SubscriptionResponse(
                        findIdByStreamKey(StreamKey.from(req)).orElse(0L),
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
            throw new IllegalStateException("Failed to serialize Kucoin message: " + msg, e);
        }
    }

    private KucoinOutgoingMsg resolveMsg(SubscriptionDto request, Action action) {

        String id = Long.toString(StreamKey.from(request).hashCode() & 0xFFFFFFFFL);
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

    private GroupPoolActor getOrCreatePool(GroupKey groupKey) {
        return pools.computeIfAbsent(groupKey, key -> {
            GroupPoolActor poolActor = new GroupPoolActor(
                    key,
                    adapter,
                    wsClient,
                    rawPublisher,
                    sub -> translate(sub, Action.SUBSCRIBE),
                    sub -> translate(sub, Action.UNSUBSCRIBE)
            );
            poolActor.start();
            return poolActor;
        });
    }

    @Override
    public void shutdown() {
        pools.values().forEach(GroupPoolActor::close);
        pools.clear();
    }
}