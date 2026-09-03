package ru.connector.exchange.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import ru.connector.command.MarketType;
import ru.connector.command.SubscriptionKey;
import ru.connector.transport.KafkaPublisher;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExchangeConnection {

    private static final Logger log = LoggerFactory.getLogger(ExchangeConnection.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    public static final int DEFAULT_MAX_SUBSCRIPTIONS = 100;

    private final String exchangeName;
    private final MarketType market;
    private final String url;
    private final WebSocketClient client;
    private final KafkaPublisher publisher;
    private final String kafkaKeyPrefix;
    private final Duration pingInterval;
    private final int maxSubscriptions;

    private final Map<SubscriptionKey, String> activeSubscriptions = new ConcurrentHashMap<>();
    private final Sinks.Many<String> outgoing = Sinks.many().multicast().onBackpressureBuffer();

    private Disposable subscription;
    private volatile boolean closed = false;

    public ExchangeConnection(String exchangeName,
                              MarketType market,
                              String url,
                              WebSocketClient client,
                              KafkaPublisher publisher,
                              String kafkaKeyPrefix,
                              Duration pingInterval) {
        this(exchangeName, market, url, client, publisher, kafkaKeyPrefix, pingInterval, DEFAULT_MAX_SUBSCRIPTIONS);
    }

    public ExchangeConnection(String exchangeName,
                              MarketType market,
                              String url,
                              WebSocketClient client,
                              KafkaPublisher publisher,
                              String kafkaKeyPrefix,
                              Duration pingInterval,
                              int maxSubscriptions) {
        this.exchangeName = exchangeName;
        this.market = market;
        this.url = url;
        this.client = client;
        this.publisher = publisher;
        this.kafkaKeyPrefix = kafkaKeyPrefix;
        this.pingInterval = pingInterval;
        this.maxSubscriptions = maxSubscriptions > 0 ? maxSubscriptions : DEFAULT_MAX_SUBSCRIPTIONS;
    }

    public void start() {
        if (closed) {
            throw new IllegalStateException("Cannot start closed connection [" + exchangeName + "]");
        }
        subscription = run().subscribe(
                null,
                err -> log.error("[{}] Connection loop terminated", exchangeName, err)
        );
    }

    private Flux<Void> run() {
        return Mono.defer(this::connectOnce)
                .doOnError(e -> log.error("[{}] connect error, will retry", exchangeName, e))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30)).jitter(0.2))
                .repeat();
    }

    private Mono<Void> connectOnce() {
        return client.execute(URI.create(url), this::handleSession)
                .doOnSubscribe(s -> log.info("[{}] Connecting to {}", exchangeName, url))
                .doOnSuccess(v -> log.info("[{}] Disconnected from {}", exchangeName, url));
    }

    private Mono<Void> handleSession(WebSocketSession session) {
        Flux<WebSocketMessage> outgoingFlux = Flux.merge(
                        Flux.defer(() -> Flux.fromIterable(activeSubscriptions.values()).map(session::textMessage)),
                        outgoing.asFlux().map(session::textMessage),
                        Flux.interval(pingInterval).map(i -> session.textMessage(pingPayload())))
                .doOnError(e -> log.error("[{}] outgoing error:", exchangeName, e));

        Mono<Void> sendTask = session.send(outgoingFlux).then();

        Mono<Void> receiveTask = session.receive()
                .map(ExchangeConnection::toRawBytes)
                .doOnNext(bytes -> publisher.publish(kafkaKeyPrefix, bytes))
                .doOnError(e -> log.error("[{}] incoming error:", exchangeName, e))
                .then();

        return Mono.firstWithSignal(sendTask, receiveTask).then();
    }

    public void subscribe(SubscriptionKey key, String payload) {
        activeSubscriptions.put(key, payload);
        outgoing.tryEmitNext(payload);
    }

    public void unsubscribe(SubscriptionKey key, String payload) {
        activeSubscriptions.remove(key);
        outgoing.tryEmitNext(payload);
    }

    public int subscriptionCount() {
        return activeSubscriptions.size();
    }

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
        log.info("[{}] Physical connection shut down for {}", exchangeName, url);
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