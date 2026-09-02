package ru.connector.exchange;

import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.command.MarketType;
import ru.connector.command.Type;
import ru.connector.command.Request;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.transport.KafkaPublisher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractWebsocketManager implements ExchangeManager {

    protected final WebSocketClient client;
    protected final KafkaPublisher publisher;

    protected final Map<MarketType, Map<Type, ExchangeConnection>> connections = new ConcurrentHashMap<>();

    public AbstractWebsocketManager(WebSocketClient client, KafkaPublisher publisher) {
        this.client = client;
        this.publisher = publisher;
    }

    public abstract String translate(Request request);

    @Override
    public void shutdown() {
        connections.values().forEach(map ->
                map.values().forEach(ExchangeConnection::shutdown));
        connections.clear();
    }
}