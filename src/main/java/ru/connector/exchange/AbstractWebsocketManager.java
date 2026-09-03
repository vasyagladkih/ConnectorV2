package ru.connector.exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.command.GroupKey;
import ru.connector.command.Request;
import ru.connector.command.SubscriptionKey;
import ru.connector.command.Type;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.transport.KafkaPublisher;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractWebsocketManager implements ExchangeManager {

    private static final Logger log = LoggerFactory.getLogger(AbstractWebsocketManager.class);

    protected final WebSocketClient client;
    protected final KafkaPublisher publisher;
    protected final Duration defaultIdleTimeout;

    protected final Map<GroupKey, ConnectionGroup> groups = new ConcurrentHashMap<>();

    public AbstractWebsocketManager(WebSocketClient client, KafkaPublisher publisher) {
        this(client, publisher, ConnectionGroup.DEFAULT_IDLE_TIMEOUT);
    }

    public AbstractWebsocketManager(WebSocketClient client, KafkaPublisher publisher, Duration defaultIdleTimeout) {
        this.client = client;
        this.publisher = publisher;
        this.defaultIdleTimeout = defaultIdleTimeout;
    }

    public abstract String translate(Request request);

    protected abstract ExchangeConnection createConnection(GroupKey groupKey);

    protected ConnectionGroup getOrCreateGroup(GroupKey groupKey) {
        return groups.computeIfAbsent(groupKey, gk ->
                new ConnectionGroup(gk, () -> createConnection(gk), defaultIdleTimeout));
    }

    @Override
    public void subscribe(Request request) {
        GroupKey groupKey = GroupKey.of(request.market(), Type.type(request.command()));
        SubscriptionKey subKey = SubscriptionKey.from(request);
        String payload = translate(request);
        ConnectionGroup g = getOrCreateGroup(groupKey);
        g.subscribe(subKey, payload);
    }

    @Override
    public void unsubscribe(Request request) {
        GroupKey groupKey = GroupKey.of(request.market(), Type.type(request.command()));
        SubscriptionKey subKey = SubscriptionKey.from(request);
        ConnectionGroup group = groups.get(groupKey);
        if (group != null) {
            String payload = translate(request);
            group.unsubscribe(subKey, payload);
        }
    }

    @Override
    public void shutdown() {
        log.info("Shutting down AbstractWebsocketManager: closing {} connection groups", groups.size());
        groups.values().forEach(ConnectionGroup::shutdown);
        groups.clear();
    }

    public Map<GroupKey, ConnectionGroup> getGroups() {
        return Collections.unmodifiableMap(groups);
    }
}