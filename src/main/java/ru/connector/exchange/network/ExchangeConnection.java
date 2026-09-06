package ru.connector.exchange.network;

import lombok.Getter;
import org.jspecify.annotations.NullMarked;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.util.function.Consumer;

@NullMarked
public class ExchangeConnection implements WebSocketHandler {

    private static final int MAX_CAPACITY = 100;

    @Getter
    private final URI url;
    private final WebSocketClient client;
    private final Consumer<byte[]> onMessage;

    private final Sinks.Many<String> outgoing = Sinks.many().multicast().onBackpressureBuffer(1024, false);
    private final Sinks.One<Void> connected = Sinks.one();

    @Getter
    private int activeSlots = 0;
    @Getter
    private boolean closed = false;
    private boolean started = false;

    public ExchangeConnection(URI url, WebSocketClient client, Consumer<byte[]> onMessage) {
        this.url = url;
        this.client = client;
        this.onMessage = onMessage;
    }

    public void start() {
        if (!started) {
            started = true;
            client.execute(url, this)
                    .doOnError(connected::tryEmitError)
                    .subscribe();
        }
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        connected.tryEmitEmpty();

        Mono<Void> inbound = session.receive()
                .doOnNext(message -> {
                    DataBuffer buffer = message.getPayload();
                    try {
                        byte[] bytes = new byte[buffer.readableByteCount()];
                        buffer.read(bytes);
                        onMessage.accept(bytes);
                    } finally {
                        DataBufferUtils.release(buffer);
                    }
                })
                .then();

        Mono<Void> outbound = session.send(outgoing.asFlux().map(session::textMessage));

        return Mono.firstWithSignal(inbound, outbound).then().doFinally(_ -> close());
    }

    public Mono<Void> send(String frame) {
        if (closed) {
            return Mono.error(new IllegalStateException("Socket is closed: " + url));
        }
        start();
        return connected.asMono().then(Mono.fromRunnable(() -> outgoing.tryEmitNext(frame)));
    }

    public boolean hasCapacity() {
        return !closed && activeSlots < MAX_CAPACITY;
    }

    public void acquireSlot() {
        activeSlots++;
    }

    public void releaseSlot() {
        if (activeSlots > 0) activeSlots--;
        if (activeSlots <= 0) close();
    }

    public void close() {
        closed = true;
        outgoing.tryEmitComplete();
    }
}