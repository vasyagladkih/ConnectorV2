package ru.connector.exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import ru.connector.command.GroupKey;
import ru.connector.command.SubscriptionKey;
import ru.connector.exchange.network.ExchangeConnection;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ConnectionGroup {

    private static final Logger log = LoggerFactory.getLogger(ConnectionGroup.class);
    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(60);

    private final GroupKey groupKey;
    private final Supplier<ExchangeConnection> connectionFactory;
    private final Duration idleTimeout;

    private final Map<SubscriptionKey, ExchangeConnection> assignments = new ConcurrentHashMap<>();
    private final Map<ExchangeConnection, Disposable> idleConnections = new ConcurrentHashMap<>();

    public ConnectionGroup(GroupKey groupKey, Supplier<ExchangeConnection> connectionFactory) {
        this(groupKey, connectionFactory, DEFAULT_IDLE_TIMEOUT);
    }

    public ConnectionGroup(GroupKey groupKey,
                           Supplier<ExchangeConnection> connectionFactory,
                           Duration idleTimeout) {
        this.groupKey = Objects.requireNonNull(groupKey, "GroupKey cannot be null");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "ConnectionFactory cannot be null");
        this.idleTimeout = idleTimeout != null ? idleTimeout : DEFAULT_IDLE_TIMEOUT;
    }

    public void subscribe(SubscriptionKey key, String payload) {
        Objects.requireNonNull(key, "SubscriptionKey cannot be null");
        Objects.requireNonNull(payload, "Payload cannot be null");

        ExchangeConnection conn;
        synchronized (this) {
            if (assignments.containsKey(key)) {
                log.debug("[{}] Subscription already active for {}, ignoring duplicate subscribe", groupKey, key);
                return;
            }

            conn = assignments.values().stream()
                    .distinct()
                    .filter(ExchangeConnection::hasCapacity)
                    .min(Comparator.comparingInt(ExchangeConnection::subscriptionCount))
                    .orElse(null);

            if (conn == null && !idleConnections.isEmpty()) {
                Iterator<Map.Entry<ExchangeConnection, Disposable>> it = idleConnections.entrySet().iterator();
                if (it.hasNext()) {
                    Map.Entry<ExchangeConnection, Disposable> idleEntry = it.next();
                    conn = idleEntry.getKey();
                    Disposable timerTask = idleEntry.getValue();
                    if (timerTask != null) {
                        timerTask.dispose();
                    }
                    it.remove();
                    log.info("[{}] Reusing idle physical connection for subscription: {}", groupKey, key);
                }
            }

            if (conn == null) {
                conn = connectionFactory.get();
                log.info("[{}] Creating new physical connection (capacity: {}) for {}",
                        groupKey, conn.getMaxSubscriptions(), key);
                conn.start();
            }

            assignments.put(key, conn);
        }

        conn.subscribe(key, payload);
    }

    public void unsubscribe(SubscriptionKey key, String payload) {
        Objects.requireNonNull(key, "SubscriptionKey cannot be null");

        ExchangeConnection conn;
        synchronized (this) {
            conn = assignments.remove(key);
            if (conn == null) {
                log.debug("[{}] Subscription not found for {}, ignoring unsubscribe", groupKey, key);
                return;
            }

            conn.unsubscribe(key, payload);

            if (conn.subscriptionCount() == 0) {
                markIdle(conn);
            }
        }
    }

    private void markIdle(ExchangeConnection conn) {
        if (idleTimeout.isZero() || idleTimeout.isNegative()) {
            log.info("[{}] Immediate close for idle connection (timeout is zero)", groupKey);
            conn.shutdown();
            return;
        }

        log.info("[{}] Physical connection became idle (0 subscriptions). Timeout: {}", groupKey, idleTimeout);
        Disposable timeoutTask = Mono.delay(idleTimeout)
                .subscribe(_ -> closeIdleConnection(conn));
        idleConnections.put(conn, timeoutTask);
    }

    private void closeIdleConnection(ExchangeConnection conn) {
        synchronized (this) {
            Disposable task = idleConnections.remove(conn);
            if (task != null) {
                task.dispose();
            }
            if (conn.subscriptionCount() == 0 && !assignments.containsValue(conn)) {
                log.info("[{}] Closing idle physical connection after timeout expiration", groupKey);
                conn.shutdown();
            }
        }
    }

    public synchronized int subscriptionCount() {
        return assignments.size();
    }

    public synchronized int physicalConnectionCount() {
        Set<ExchangeConnection> all = new HashSet<>(assignments.values());
        all.addAll(idleConnections.keySet());
        return all.size();
    }

    public synchronized Set<ExchangeConnection> getActiveConnections() {
        return new HashSet<>(assignments.values());
    }

    public synchronized Set<ExchangeConnection> getIdleConnections() {
        return Collections.unmodifiableSet(idleConnections.keySet());
    }

    public Map<SubscriptionKey, ExchangeConnection> getAssignments() {
        return Collections.unmodifiableMap(assignments);
    }

    public GroupKey getGroupKey() {
        return groupKey;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public synchronized void shutdown() {
        for (Disposable timer : idleConnections.values()) {
            if (timer != null) {
                timer.dispose();
            }
        }
        for (ExchangeConnection conn : idleConnections.keySet()) {
            conn.shutdown();
        }
        idleConnections.clear();

        assignments.values().stream().distinct().forEach(ExchangeConnection::shutdown);
        assignments.clear();
        log.info("[{}] ConnectionGroup shut down completely", groupKey);
    }
}
