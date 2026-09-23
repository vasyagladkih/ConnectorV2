package ru.connector.exchange.actor;

import lombok.Getter;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import ru.connector.exchange.network.ExchangeAdapter;
import ru.connector.models.StreamKey;

import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Реактивный актор отдельного WebSocket-соединения.
 * <p>
 * Изолирует состояние подписок, автоматический реконнект с backoff, rate-limiting
 * и потокобезопасную передачу команд через однопоточный планировщик.
 */
public final class ConnectionActor {

    private static final Logger log = LoggerFactory.getLogger(ConnectionActor.class);
    private static final int DEFAULT_BUFFER_SIZE = 1024;

    @Getter
    private final int id;
    private final URI exchangeUrl;
    private final ExchangeAdapter adapter;
    private final WebSocketClient wsClient;
    private final Consumer<byte[]> dataConsumer;
    private final Consumer<Throwable> fatalErrorConsumer;
    private final Duration readIdleTimeout;
    private final Duration rateLimitDelay;
    private final Duration reconnectDelay;

    private final Sinks.Many<ConnectionCommand> mailbox = Sinks.many().unicast().onBackpressureBuffer();
    private final Scheduler scheduler;
    private final Sinks.Many<String> outboundSink = Sinks.many().multicast().onBackpressureBuffer(DEFAULT_BUFFER_SIZE, false);
    private final Map<StreamKey, String> activeSubscriptions = new ConcurrentHashMap<>();
    private final Sinks.One<Void> connected = Sinks.one();
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    @Getter
    private volatile boolean closed = false;
    private Disposable connection;
    private Disposable mailboxLoop;

    ConnectionActor(ConnectionActorBuilder builder) {
        this.id = builder.getId();
        this.exchangeUrl = builder.getExchangeUrl();
        this.adapter = builder.getAdapter();
        this.wsClient = builder.getWsClient();
        this.dataConsumer = builder.getDataConsumer();
        this.fatalErrorConsumer = builder.getFatalErrorConsumer();
        this.readIdleTimeout = builder.getReadIdleTimeout();
        this.rateLimitDelay = builder.getRateLimitDelay();
        this.reconnectDelay = builder.getReconnectDelay();
        this.scheduler = Schedulers.newSingle("conn-actor-" + id);
    }

    public static ConnectionActorBuilder builder() {return new ConnectionActorBuilder();}

    public void start() {
        if (closed || !isStarted.compareAndSet(false, true)) return;

        this.mailboxLoop = mailbox.asFlux()
                .publishOn(scheduler)
                .concatMap(this::handleCommand)
                .subscribe();

        WebSocketHandler handler = session -> {
            connected.tryEmitEmpty();
            Mono<Void> inbound = inboundDataStream(session);
            Flux<WebSocketMessage> outboundCommands = allOutBoundStream(session);

            Flux<WebSocketMessage> allOutbound = Flux.merge(
                    outboundCommands,
                    adapter.createPingStream(session)
            );
            return session.send(allOutbound).and(inbound);
        };

        this.connection = wsClient.execute(exchangeUrl, handler)
                .doOnError(connected::tryEmitError)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(200))
                        .maxBackoff(Duration.ofMinutes(1))
                        .filter(adapter::isRecoverable))
                .repeatWhen(completed -> completed.delayElements(reconnectDelay))
                .doOnError(err -> {
                    log.error("Фатальная ошибка WebSocket-соединения {}", id, err);
                    fatalErrorConsumer.accept(err);
                })
                .subscribe();
    }

    private @NonNull Flux<WebSocketMessage> allOutBoundStream(WebSocketSession session) {
        Flux<String> outboundFlux = outboundSink.asFlux();
        Flux<String> commands;

        if (isConnected.compareAndSet(false, true)) {
            commands = outboundFlux;
        } else {
            List<String> snapshot = new ArrayList<>(activeSubscriptions.values());
            Flux<String> resubscribeStream = Flux.fromIterable(snapshot);
            commands = Flux.concat(resubscribeStream, outboundFlux);
        }

        if (!rateLimitDelay.isZero()) {
            commands = commands.delayElements(rateLimitDelay);
        }
        return commands.map(session::textMessage);
    }

    private @NonNull Mono<Void> inboundDataStream(WebSocketSession session) {
        return session.receive()
                .timeout(readIdleTimeout)
                .map(WebSocketMessage::getPayload)
                .doOnNext(buffer -> {
                    try {
                        if (adapter.isServiceMsg(buffer)) {
                            adapter.handleServiceMsg(buffer);
                        } else {
                            byte[] bytes = new byte[buffer.readableByteCount()];
                            buffer.read(bytes);
                            dataConsumer.accept(bytes);
                        }
                    } catch (Exception e) {
                        log.error("Ошибка обработки входящего фрейма в сокете {}", id, e);
                    } finally {
                        DataBufferUtils.release(buffer);
                    }
                }).then();
    }

    private Mono<Void> handleCommand(ConnectionCommand command) {
        return switch (command) {
            case ConnectionCommand.Subscribe sub -> handleSubscribe(sub);
            case ConnectionCommand.Unsubscribe unsub -> handleUnsubscribe(unsub);
            case ConnectionCommand.Close closeCmd -> handleClose(closeCmd);
        };
    }

    private Mono<Void> handleSubscribe(ConnectionCommand.Subscribe cmd) {
        if (closed) {
            cmd.reply().emitError(new IllegalStateException(MessageFormat.format("Соединение {0} закрыто", id)), Sinks.EmitFailureHandler.FAIL_FAST);
            return Mono.empty();
        }

        activeSubscriptions.put(cmd.key(), cmd.frame());
        Sinks.EmitResult result = outboundSink.tryEmitNext(cmd.frame());
        if (result.isFailure()) {
            activeSubscriptions.remove(cmd.key());
            cmd.reply().emitError(new IllegalStateException("Не удалось отправить фрейм подписки: " + result), Sinks.EmitFailureHandler.FAIL_FAST);
            return Mono.empty();
        }
        cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        return Mono.empty();
    }

    private Mono<Void> handleUnsubscribe(ConnectionCommand.Unsubscribe cmd) {
        if (closed) {
            cmd.reply().emitError(new IllegalStateException(MessageFormat.format("Соединение {0} закрыто", id)), Sinks.EmitFailureHandler.FAIL_FAST);
            return Mono.empty();
        }

        String removed = activeSubscriptions.remove(cmd.key());
        if (removed != null) {
            Sinks.EmitResult result = outboundSink.tryEmitNext(cmd.unsubscribeFrame());
            if (result.isFailure()) {
                activeSubscriptions.put(cmd.key(), removed);
                cmd.reply().emitError(new IllegalStateException("Не удалось отправить фрейм отписки: " + result), Sinks.EmitFailureHandler.FAIL_FAST);
                return Mono.empty();
            }
        }
        cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        return Mono.empty();
    }

    private Mono<Void> handleClose(ConnectionCommand.Close cmd) {
        close();
        cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        return Mono.empty();
    }

    public Mono<Void> subscribe(StreamKey key, String frame) {
        if (closed) return Mono.error(new IllegalStateException("Соединение " + id + " закрыто"));
        return connected.asMono().then(Mono.defer(() -> {
            Sinks.One<Void> reply = Sinks.one();
            Sinks.EmitResult result = mailbox.tryEmitNext(new ConnectionCommand.Subscribe(key, frame, reply));
            if (result.isFailure()) {
                return Mono.error(new IllegalStateException("Очередь актора отклонила подписку: " + result));
            }
            return reply.asMono();
        }));
    }

    public Mono<Void> unsubscribe(StreamKey key, String unsubscribeFrame) {
        if (closed) return Mono.error(new IllegalStateException("Соединение " + id + " закрыто"));
        Sinks.One<Void> reply = Sinks.one();
        Sinks.EmitResult result = mailbox.tryEmitNext(new ConnectionCommand.Unsubscribe(key, unsubscribeFrame, reply));
        if (result.isFailure()) {
            return Mono.error(new IllegalStateException("Очередь актора отклонила отписку: " + result));
        }
        return reply.asMono();
    }

    public void close() {
        if (closed) return;
        closed = true;
        connected.tryEmitEmpty();
        if (connection != null && !connection.isDisposed()) connection.dispose();
        if (mailboxLoop != null && !mailboxLoop.isDisposed()) mailboxLoop.dispose();
        outboundSink.tryEmitComplete();
        mailbox.tryEmitComplete();
        scheduler.dispose();
    }
}