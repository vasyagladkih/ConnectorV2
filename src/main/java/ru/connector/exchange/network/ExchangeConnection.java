package ru.connector.exchange.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import ru.connector.exceptions.ConnectionCapacityException;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.MarketType;
import ru.connector.models.SubscriptionKey;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExchangeConnection {

    private static final ObjectMapper mapper = new ObjectMapper();
    public static final int DEFAULT_MAX_SUBSCRIPTIONS = 100;
    public static final long DEFAULT_SEND_DELAY_MS = 50L;

    private final String exchangeName;
    private final MarketType market;
    private final String url;
    private final WebSocketClient client;
    private final KafkaRawDataPublisher publisher;
    private final String kafkaKeyPrefix;
    private final Duration pingInterval;
    private final int maxSubscriptions;
    private final long sendDelayMs;

    private final Map<SubscriptionKey, String> activeSubscriptions = new ConcurrentHashMap<>();
    private final Sinks.Many<String> outgoing = Sinks.many().multicast().onBackpressureBuffer(1000, false);
    private final AtomicBoolean isReconnecting = new AtomicBoolean(false);

    private Disposable subscription;
    private volatile boolean closed = false;

    public ExchangeConnection(String exchangeName,
                              MarketType market,
                              String url,
                              WebSocketClient client,
                              KafkaRawDataPublisher publisher,
                              String kafkaKeyPrefix,
                              Duration pingInterval) {
        this(exchangeName, market, url, client, publisher, kafkaKeyPrefix, pingInterval, DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_SEND_DELAY_MS);
    }

    public ExchangeConnection(String exchangeName,
                              MarketType market,
                              String url,
                              WebSocketClient client,
                              KafkaRawDataPublisher publisher,
                              String kafkaKeyPrefix,
                              Duration pingInterval,
                              int maxSubscriptions) {
        this(exchangeName, market, url, client, publisher, kafkaKeyPrefix, pingInterval, maxSubscriptions, DEFAULT_SEND_DELAY_MS);
    }

    public ExchangeConnection(String exchangeName,
                              MarketType market,
                              String url,
                              WebSocketClient client,
                              KafkaRawDataPublisher publisher,
                              String kafkaKeyPrefix,
                              Duration pingInterval,
                              int maxSubscriptions,
                              long sendDelayMs) {
        this.exchangeName = exchangeName;
        this.market = market;
        this.url = url;
        this.client = client;
        this.publisher = publisher;
        this.kafkaKeyPrefix = kafkaKeyPrefix;
        this.pingInterval = pingInterval;
        this.maxSubscriptions = maxSubscriptions > 0 ? maxSubscriptions : DEFAULT_MAX_SUBSCRIPTIONS;
        this.sendDelayMs = sendDelayMs >= 0 ? sendDelayMs : DEFAULT_SEND_DELAY_MS;
    }

    public void start() {
        if (closed) {
            throw new IllegalStateException("Cannot start closed connection [" + exchangeName + "]");
        }
        subscription = run().subscribe();
    }

    private Flux<Void> run() {
        return Mono.defer(this::connectOnce)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30)).jitter(0.2))
                .repeat();
    }

    private Mono<Void> connectOnce() {
        return client.execute(URI.create(url), this::handleSession);
    }

    private Mono<Void> handleSession(WebSocketSession session) {
        Flux<String> resubscribeFlux = isReconnecting.getAndSet(true)
                ? Flux.fromIterable(activeSubscriptions.values())
                : Flux.empty();

        Flux<WebSocketMessage> outgoingFlux = Flux.merge(
                resubscribeFlux.map(session::textMessage),
                outgoing.asFlux().delayElements(Duration.ofMillis(sendDelayMs)).map(session::textMessage),
                Flux.interval(pingInterval).map(_ -> session.textMessage(pingPayload()))
        );

        Mono<Void> sendTask = session.send(outgoingFlux).then();

        Mono<Void> receiveTask = session.receive()
                .map(ExchangeConnection::toRawBytes)
                .doOnNext(bytes -> publisher.publish(kafkaKeyPrefix, bytes))
                .then();

        return Mono.firstWithSignal(sendTask, receiveTask).then();
    }

    public synchronized boolean tryReserveSlot(SubscriptionKey key, String payload) {
        if (activeSubscriptions.size() >= maxSubscriptions) {
            return false;
        }
        activeSubscriptions.put(key, payload);
        return true;
    }

    public void subscribe(SubscriptionKey key, String payload) {
        activeSubscriptions.put(key, payload);
        emitOutgoing(payload);
    }

    public void unsubscribe(SubscriptionKey key, String payload) {
        activeSubscriptions.remove(key);
        emitOutgoing(payload);
    }

    public void emitOutgoing(String payload) {
        Sinks.EmitResult result = outgoing.tryEmitNext(payload);
        if (result.isFailure()) {
            throw new ConnectionCapacityException("Outgoing buffer overflow for exchange [" + exchangeName + "]");
        }
    }

    public int subscriptionCount() {return activeSubscriptions.size();}

    public boolean hasCapacity() {
        return activeSubscriptions.size() < maxSubscriptions;
    }

    public int getMaxSubscriptions() {
        return maxSubscriptions;
    }

    public Map<SubscriptionKey, String> getActiveSubscriptions() {
        return Collections.unmodifiableMap(activeSubscriptions);
    }

    public boolean isClosed() {
        return closed;
    }

    public String getUrl() {
        return url;
    }

    public MarketType getMarket() {
        return market;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public long getSendDelayMs() {
        return sendDelayMs;
    }

    private String pingPayload() {
        try {
            return mapper.writeValueAsString(Map.of("id", System.currentTimeMillis(), "type", "ping"));
        } catch (Exception e) {
            return "{\"type\":\"ping\"}";
        }
    }

    public void shutdown() {
        closed = true;
        if (subscription != null) {
            subscription.dispose();
        }
        outgoing.tryEmitComplete();
    }

    public static byte[] toRawBytes(WebSocketMessage msg) {
        DataBuffer payload = msg.getPayload();
        try {
            int readable = payload.readableByteCount();
            byte[] bytes = new byte[readable];
            payload.read(bytes);
            return bytes;
        } finally {
            DataBufferUtils.release(payload);
        }
    }
}