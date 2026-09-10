package ru.connector.exchange.network;

import lombok.Getter;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import ru.connector.models.StreamKey;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@NullMarked
public class ExchangeConnection implements WebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ExchangeConnection.class);
    private static final int MAX_CAPACITY = 100;
    private static final Duration DEFAULT_PING_INTERVAL = Duration.ofSeconds(18);

    @Getter
    private final URI url;
    private final WebSocketClient client;
    private final Consumer<byte[]> onMessage;
    private final Duration pingInterval;

    private final Sinks.Many<String> outgoing = Sinks.many().multicast().onBackpressureBuffer(1024, false);
    private final Sinks.One<Void> connected = Sinks.one();

    private final Set<StreamKey> activeStreams = ConcurrentHashMap.newKeySet();
    private final AtomicInteger activeSlots = new AtomicInteger(0);
    @Getter
    private volatile boolean closed = false;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile @Nullable WebSocketSession session;

    public ExchangeConnection(URI url, WebSocketClient client, Consumer<byte[]> onMessage) {
        this(url, client, onMessage, DEFAULT_PING_INTERVAL);
    }

    public ExchangeConnection(URI url, WebSocketClient client, Consumer<byte[]> onMessage, Duration pingInterval) {
        this.url = url;
        this.client = client;
        this.onMessage = onMessage;
        this.pingInterval = pingInterval;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            client.execute(url, this)
                    .doOnError(connected::tryEmitError)
                    .subscribe();
        }
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        this.session = session;
        connected.tryEmitEmpty();

        Mono<Void> inbound = session.receive()
                .doOnNext(message -> {
                    DataBuffer buffer = message.getPayload();
                    try {
                        byte[] bytes = new byte[buffer.readableByteCount()];
                        buffer.read(bytes);
                        if (!isControlFrame(bytes)) {
                            try {
                                onMessage.accept(bytes);
                            } catch (Throwable t) {
                                log.error("Error handling incoming WebSocket frame: {}", t.getMessage(), t);
                            }
                        }
                    } finally {
                        DataBufferUtils.release(buffer);
                    }
                })
                .then();

        Flux<String> pingFlux = Flux.interval(pingInterval)
                .takeWhile(_ -> !closed)
                .map(_ -> "{\"id\":\"" + System.currentTimeMillis() + "\",\"op\":\"ping\"}");

        Flux<String> combinedOutgoing = Flux.merge(outgoing.asFlux(), pingFlux);
        Mono<Void> outbound = session.send(combinedOutgoing.map(session::textMessage));

        return Mono.firstWithSignal(inbound, outbound)
                .then()
                .doFinally(_ -> close());
    }

    public Mono<Void> send(String frame) {
        if (closed) {
            return Mono.error(new IllegalStateException("Socket is closed: " + url));
        }
        start();
        return connected.asMono().then(Mono.defer(() -> {
            Sinks.EmitResult result = outgoing.tryEmitNext(frame);
            if (result.isFailure()) {
                return Mono.error(new IllegalStateException("Failed to emit frame into outgoing buffer: " + result));
            }
            return Mono.empty();
        }));
    }

    public boolean hasCapacity() {
        return !closed && getActiveSlots() < MAX_CAPACITY;
    }

    public int getActiveSlots() {
        return activeStreams.isEmpty() ? activeSlots.get() : activeStreams.size();
    }

    public boolean acquireSlot(StreamKey key) {
        if (closed || getActiveSlots() >= MAX_CAPACITY) return false;
        boolean added = activeStreams.add(key);
        if (added) {
            activeSlots.set(activeStreams.size());
        }
        return added;
    }

    public boolean acquireSlot() {
        if (closed) return false;
        int prev = activeSlots.getAndUpdate(cur -> (cur < MAX_CAPACITY && !closed) ? cur + 1 : cur);
        return prev < MAX_CAPACITY && !closed;
    }

    public void releaseSlot(StreamKey key) {
        if (activeStreams.remove(key)) {
            activeSlots.set(activeStreams.size());
        }
    }

    public void releaseSlot() {
        activeSlots.updateAndGet(cur -> cur > 0 ? cur - 1 : 0);
    }

    public void close() {
        closed = true;
        outgoing.tryEmitComplete();
        WebSocketSession currentSession = this.session;
        if (currentSession != null) {
            Mono<Void> closeMono = currentSession.close();
            if (closeMono != null) {
                closeMono.subscribe();
            }
        }
    }

    private boolean isControlFrame(byte[] bytes) {
        String str = new String(bytes, StandardCharsets.UTF_8);
        return str.contains("\"welcome\"") || str.contains("\"pong\"");
    }
}
