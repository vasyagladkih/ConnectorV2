package ru.connector.exchange.actor;

import lombok.Getter;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.exchange.network.ExchangeAdapter;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Строитель экземпляров {@link ConnectionActor}.
 */
@Getter
public class ConnectionActorBuilder {

    private static final Duration DEFAULT_READ_IDLE_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_RATE_LIMIT_DELAY = Duration.ZERO;
    private static final Duration DEFAULT_RECONNECT_DELAY = Duration.ofSeconds(1);

    private int id;
    private URI exchangeUrl;
    private ExchangeAdapter adapter;
    private WebSocketClient wsClient;
    private Consumer<byte[]> dataConsumer;
    private Consumer<Throwable> fatalErrorConsumer;
    private Duration readIdleTimeout = DEFAULT_READ_IDLE_TIMEOUT;
    private Duration rateLimitDelay = DEFAULT_RATE_LIMIT_DELAY;
    private Duration reconnectDelay = DEFAULT_RECONNECT_DELAY;

    public ConnectionActorBuilder id(int id) {
        this.id = id;
        return this;
    }

    public ConnectionActorBuilder exchangeUrl(URI exchangeUrl) {
        this.exchangeUrl = exchangeUrl;
        return this;
    }

    public ConnectionActorBuilder adapter(ExchangeAdapter adapter) {
        this.adapter = adapter;
        return this;
    }

    public ConnectionActorBuilder wsClient(WebSocketClient wsClient) {
        this.wsClient = wsClient;
        return this;
    }

    public ConnectionActorBuilder dataConsumer(Consumer<byte[]> dataConsumer) {
        this.dataConsumer = dataConsumer;
        return this;
    }

    public ConnectionActorBuilder fatalErrorConsumer(Consumer<Throwable> fatalErrorConsumer) {
        this.fatalErrorConsumer = fatalErrorConsumer;
        return this;
    }

    @SuppressWarnings("unused")
    public ConnectionActorBuilder readIdleTimeout(Duration readIdleTimeout) {
        if (readIdleTimeout != null) {
            this.readIdleTimeout = readIdleTimeout;
        }
        return this;
    }

    @SuppressWarnings("unused")
    public ConnectionActorBuilder rateLimitDelay(Duration rateLimitDelay) {
        if (rateLimitDelay != null) {
            this.rateLimitDelay = rateLimitDelay;
        }
        return this;
    }

    @SuppressWarnings("unused")
    public ConnectionActorBuilder reconnectDelay(Duration reconnectDelay) {
        if (reconnectDelay != null) {
            this.reconnectDelay = reconnectDelay;
        }
        return this;
    }

    /**
     * Создает экземпляр ConnectionActor с проверкой обязательных параметров.
     */
    public ConnectionActor build() {
        Objects.requireNonNull(exchangeUrl, "exchangeUrl cannot be null");
        Objects.requireNonNull(adapter, "adapter cannot be null");
        Objects.requireNonNull(wsClient, "wsClient cannot be null");
        Objects.requireNonNull(dataConsumer, "dataConsumer cannot be null");
        Objects.requireNonNull(fatalErrorConsumer, "fatalErrorConsumer cannot be null");
        return new ConnectionActor(this);
    }
}