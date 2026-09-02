package ru.connector.exchange.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import ru.connector.command.MarketType;
import ru.connector.command.StreamKey;
import ru.connector.transport.KafkaPublisher;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExchangeConnection {

    private static final Logger log = LoggerFactory.getLogger(ExchangeConnection.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String exchangeName;
    private final MarketType market;
    private final String url;
    private final WebSocketClient client;
    private final KafkaPublisher publisher;
    private final String kafkaKeyPrefix;
    private final Duration pingInterval;

    // TODO: State Loss on Restart. ConcurrentHashMap is in-memory. 
    // Need to persist target subscriptions to DB (Postgres/Redis) so they can be restored on pod restart.
    private final Map<StreamKey, String> activeSubscriptions = new ConcurrentHashMap<>();

    private final Sinks.Many<String> outgoing = Sinks.many().multicast().onBackpressureBuffer();

    private Disposable subscription;

    public ExchangeConnection(String exchangeName,
                              MarketType market,
                              String url,
                              WebSocketClient client,
                              KafkaPublisher publisher,
                              String kafkaKeyPrefix,
                              Duration pingInterval) {
        this.exchangeName = exchangeName;
        this.market = market;
        this.url = url;
        this.client = client;
        this.publisher = publisher;
        this.kafkaKeyPrefix = kafkaKeyPrefix;
        this.pingInterval = pingInterval;
    }

    public void start() {
        subscription = run().subscribe(
                null,
                err -> log.error("[{}] Connection loop terminated", exchangeName, err)
        );
    }

    private Flux<Void> run() {
        // retryWhen ловит ошибки, repeat ловит штатное завершение — в обоих случаях реконнект.
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
                        // повторная подписка сохранённых подписок при (пере)подключении
                        // TODO: DDoS Risk! If reconnecting, iterating activeSubscriptions and pushing all instantly will trigger exchange IP bans. Add delayElements() for pacing.
                        Flux.defer(() -> Flux.fromIterable(activeSubscriptions.values()).map(session::textMessage)),
                        // новые subscribe/unsubscribe команды
                        outgoing.asFlux().map(session::textMessage),
                        // KuCoin защищенность соединения требует периодический ping
                        Flux.interval(pingInterval).map(i -> session.textMessage(pingPayload())))
                .doOnError(e -> log.error("[{}] outgoing error:", exchangeName, e));

        Mono<Void> sendTask = session.send(outgoingFlux).then();

        Mono<Void> receiveTask = session.receive()
                // TODO: Liveness Detection Missing. We send Pings, but we don't timeout if the server stops sending data. Add .timeout(Duration) to detect dead connections.
                .map(ExchangeConnection::toRawBytes)
                .doOnNext(bytes -> publisher.publish(kafkaKeyPrefix, bytes))
                // TODO: Backpressure/Load Shedding Missing. If Kafka lags, publisher will buffer in memory and OOM the app. Need a strategy (e.g. drop messages or disconnect).
                .doOnError(e -> log.error("[{}] incoming error:", exchangeName, e))
                .then();

        // Возвращаемся, как только одна из задач завершится (закрытие сокета), отменяя другую.
        return Mono.firstWithSignal(sendTask, receiveTask).then();
    }

    public void subscribe(StreamKey key, String payload) {
        activeSubscriptions.put(key, payload);
        outgoing.tryEmitNext(payload);
    }

    public void unsubscribe(StreamKey key, String payload) {
        activeSubscriptions.remove(key);
        outgoing.tryEmitNext(payload);
    }

    private String pingPayload() {
        try {
            return mapper.writeValueAsString(Map.of("id", System.currentTimeMillis(), "type", "ping"));
        } catch (Exception e) {
            return "{\"type\":\"ping\"}";
        }
    }

    public void shutdown() {
        if (subscription != null) subscription.dispose();
        outgoing.tryEmitComplete();
    }

    /**
     * Копирует payload сообщения из DataBuffer в новый массив байтов без промежуточной строки.
     */
    private static byte[] toRawBytes(WebSocketMessage msg) {
        // TODO: Memory Leak (CRITICAL)! Netty DataBuffer uses off-heap memory. 
        // Need to wrap read in try-finally and call DataBufferUtils.release(payload) 
        // in the finally block to prevent OutOfMemoryError: Direct buffer memory.
        DataBuffer payload = msg.getPayload();
        int readable = payload.readableByteCount();
        byte[] bytes = new byte[readable];
        payload.read(bytes);
        return bytes;
    }
}