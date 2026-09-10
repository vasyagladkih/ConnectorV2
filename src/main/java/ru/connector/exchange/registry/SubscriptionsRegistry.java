package ru.connector.exchange.registry;

import org.springframework.stereotype.Component;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.models.GroupKey;
import ru.connector.models.StreamKey;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

@Component
public class SubscriptionsRegistry {

    private final Map<GroupKey, CopyOnWriteArrayList<ExchangeConnection>> connectionsPool = new ConcurrentHashMap<>();
    private final Map<StreamKey, SubscriptionState> streamStates = new ConcurrentHashMap<>();
    private final Map<StreamKey, ExchangeConnection> streamToConnection = new ConcurrentHashMap<>();

    public boolean tryReserve(StreamKey key, SubscriptionDto request) {
        SubscriptionState pendingState = SubscriptionState.pending(request);
        return streamStates.putIfAbsent(key, pendingState) == null;
    }

    public void activate(StreamKey key, ExchangeConnection conn) {
        streamStates.computeIfPresent(key, (_, state) -> state.toActive());
        streamToConnection.put(key, conn);
    }

    public void rollback(StreamKey key) {
        streamStates.remove(key);
        streamToConnection.remove(key);
    }

    public boolean unregister(StreamKey key) {
        SubscriptionState removedState = streamStates.remove(key);
        ExchangeConnection removedConn = streamToConnection.remove(key);
        return removedState != null || removedConn != null;
    }

    public boolean exists(StreamKey key) {
        return streamStates.containsKey(key);
    }

    public Optional<SubscriptionDto> findRequestByKey(StreamKey key) {
        return Optional.ofNullable(streamStates.get(key)).map(SubscriptionState::request);
    }

    public Optional<ExchangeConnection> findConnectionByKey(StreamKey key) {
        return Optional.ofNullable(streamToConnection.get(key));
    }

    public ExchangeConnection getOrCreateConnection(GroupKey groupKey, Supplier<ExchangeConnection> factory) {
        CopyOnWriteArrayList<ExchangeConnection> connections =
                connectionsPool.computeIfAbsent(groupKey, _ -> new CopyOnWriteArrayList<>());

        connections.removeIf(ExchangeConnection::isClosed);

        for (ExchangeConnection conn : connections) {
            if (conn.hasCapacity()) return conn;
        }

        synchronized (connections) {
            for (ExchangeConnection conn : connections) {
                if (conn.hasCapacity()) return conn;
            }
            ExchangeConnection newConn = factory.get();
            connections.add(newConn);
            return newConn;
        }
    }

    public List<SubscriptionDto> getAllActive() {
        return streamStates.values().stream()
                .filter(state -> state.status() == SubscriptionState.Status.ACTIVE)
                .map(SubscriptionState::request)
                .toList();
    }

    public List<ExchangeConnection> getAllConnections() {
        return connectionsPool.values().stream().flatMap(List::stream).toList();
    }

    public void clear() {
        connectionsPool.values().forEach(list -> list.forEach(ExchangeConnection::close));
        connectionsPool.clear();
        streamStates.clear();
        streamToConnection.clear();
    }
}
