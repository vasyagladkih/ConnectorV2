package ru.connector.exchange.registry;

import org.springframework.stereotype.Component;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.models.GroupKey;
import ru.connector.models.StreamKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

@Component
public class SubscriptionsRegistry {

    private final Map<Long, SubscriptionDto> idToRequest = new HashMap<>();
    private final Map<StreamKey, Long> streamToId = new HashMap<>();
    private final Map<Long, ExchangeConnection> idToConnection = new HashMap<>();
    private final Map<GroupKey, List<ExchangeConnection>> connectionsPool = new HashMap<>();

    public boolean exists(StreamKey key) {
        return streamToId.containsKey(key);
    }

    public Optional<Long> findIdByStreamKey(StreamKey key) {
        return Optional.ofNullable(streamToId.get(key));
    }

    public Optional<SubscriptionDto> findRequestById(Long id) {
        return Optional.ofNullable(idToRequest.get(id));
    }

    public Optional<ExchangeConnection> findConnectionById(Long id) {
        return Optional.ofNullable(idToConnection.get(id));
    }

    public void register(Long id, SubscriptionDto request, ExchangeConnection conn) {
        idToRequest.put(id, request);
        streamToId.put(StreamKey.from(request), id);
        idToConnection.put(id, conn);
    }

    public void unregister(Long id) {
        SubscriptionDto req = idToRequest.remove(id);
        if (req != null) {
            streamToId.remove(StreamKey.from(req));
        }
        idToConnection.remove(id);
    }

    public ExchangeConnection getOrCreateConnection(GroupKey groupKey, Supplier<ExchangeConnection> factory) {
        List<ExchangeConnection> connections = connectionsPool.computeIfAbsent(groupKey, _ -> new ArrayList<>());
        connections.removeIf(ExchangeConnection::isClosed);

        for (ExchangeConnection conn : connections) {
            if (conn.hasCapacity()) {
                return conn;
            }
        }

        ExchangeConnection newConn = factory.get();
        connections.add(newConn);
        return newConn;
    }

    public List<SubscriptionDto> getAllActive() {
        return List.copyOf(idToRequest.values());
    }

    public List<ExchangeConnection> getAllConnections() {
        return connectionsPool.values().stream().flatMap(List::stream).toList();
    }

    public void clear() {
        connectionsPool.values().forEach(list -> list.forEach(ExchangeConnection::close));
        connectionsPool.clear();
        idToRequest.clear();
        streamToId.clear();
        idToConnection.clear();
    }
}
