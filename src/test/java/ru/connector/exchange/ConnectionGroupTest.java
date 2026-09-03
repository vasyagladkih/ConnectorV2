package ru.connector.exchange;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.models.*;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.kafka.KafkaRawDataPublisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ConnectionGroupTest {

    private WebSocketClient mockClient;
    private KafkaRawDataPublisher mockPublisher;
    private AtomicInteger connectionCounter;
    private List<ExchangeConnection> createdConnections;

    @BeforeEach
    void setUp() {
        mockClient = mock(WebSocketClient.class);
        mockPublisher = mock(KafkaRawDataPublisher.class);
        connectionCounter = new AtomicInteger(0);
        createdConnections = new ArrayList<>();
    }

    private ExchangeConnection createTestConnection(int maxSubs) {
        int id = connectionCounter.incrementAndGet();
        ExchangeConnection conn = new ExchangeConnection(
                "TEST_" + id,
                MarketType.SPOT,
                "ws://localhost/" + id,
                mockClient,
                mockPublisher,
                "TOPIC",
                Duration.ofSeconds(20),
                maxSubs
        );
        createdConnections.add(conn);
        return conn;
    }

    @Test
    void testSameGroupSharesConnectionUntilCapacityReached() {
        GroupKey groupKey = GroupKey.of(MarketType.SPOT, Type.TRADES);
        ConnectionGroup group = new ConnectionGroup(groupKey, () -> createTestConnection(3));

        SubscriptionKey subA = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        SubscriptionKey subB = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("ETH-USDT"), new Command.Trades());
        SubscriptionKey subC = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("SOL-USDT"), new Command.Trades());

        group.subscribe(subA, "{\"sub\":\"BTC\"}");
        group.subscribe(subB, "{\"sub\":\"ETH\"}");
        group.subscribe(subC, "{\"sub\":\"SOL\"}");

        assertEquals(3, group.subscriptionCount());
        assertEquals(1, group.physicalConnectionCount());
        assertEquals(1, createdConnections.size());

        ExchangeConnection firstConn = createdConnections.getFirst();
        assertEquals(3, firstConn.subscriptionCount());
        assertFalse(firstConn.hasCapacity());
    }

    @Test
    void testConnectionLimitCreatesAnotherConnection() {
        GroupKey groupKey = GroupKey.of(MarketType.SPOT, Type.TRADES);
        ConnectionGroup group = new ConnectionGroup(groupKey, () -> createTestConnection(2));

        SubscriptionKey sub1 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        SubscriptionKey sub2 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("ETH-USDT"), new Command.Trades());
        SubscriptionKey sub3 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("SOL-USDT"), new Command.Trades());

        group.subscribe(sub1, "{\"sub\":\"1\"}");
        group.subscribe(sub2, "{\"sub\":\"2\"}");

        assertEquals(1, group.physicalConnectionCount());
        assertEquals(2, createdConnections.getFirst().subscriptionCount());

        group.subscribe(sub3, "{\"sub\":\"3\"}");

        assertEquals(3, group.subscriptionCount());
        assertEquals(2, group.physicalConnectionCount());
        assertEquals(2, createdConnections.size());

        ExchangeConnection conn1 = createdConnections.get(0);
        ExchangeConnection conn2 = createdConnections.get(1);

        assertEquals(2, conn1.subscriptionCount());
        assertEquals(1, conn2.subscriptionCount());

        assertSame(conn1, group.getAssignments().get(sub1));
        assertSame(conn1, group.getAssignments().get(sub2));
        assertSame(conn2, group.getAssignments().get(sub3));
    }

    @Test
    void testDuplicateSubscribeIsIdempotent() {
        GroupKey groupKey = GroupKey.of(MarketType.SPOT, Type.TRADES);
        ConnectionGroup group = new ConnectionGroup(groupKey, () -> createTestConnection(2));

        SubscriptionKey sub1 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());

        group.subscribe(sub1, "{\"sub\":\"BTC\"}");
        assertEquals(1, group.subscriptionCount());
        assertEquals(1, createdConnections.size());

        group.subscribe(sub1, "{\"sub\":\"BTC_DUPLICATE\"}");
        assertEquals(1, group.subscriptionCount());
        assertEquals(1, createdConnections.size());
        assertEquals(1, createdConnections.getFirst().subscriptionCount());
    }

    @Test
    void testUnsubscribeRoutingAndAssignmentRemoval() {
        GroupKey groupKey = GroupKey.of(MarketType.SPOT, Type.TRADES);
        ConnectionGroup group = new ConnectionGroup(groupKey, () -> createTestConnection(10));

        SubscriptionKey sub1 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        SubscriptionKey sub2 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("ETH-USDT"), new Command.Trades());

        group.subscribe(sub1, "{\"sub\":\"1\"}");
        group.subscribe(sub2, "{\"sub\":\"2\"}");

        assertEquals(2, group.subscriptionCount());

        group.unsubscribe(sub1, "{\"unsub\":\"1\"}");

        assertEquals(1, group.subscriptionCount());
        assertFalse(group.getAssignments().containsKey(sub1));
        assertTrue(group.getAssignments().containsKey(sub2));

        ExchangeConnection conn = createdConnections.getFirst();
        assertFalse(conn.getActiveSubscriptions().containsKey(sub1));
        assertTrue(conn.getActiveSubscriptions().containsKey(sub2));

        assertDoesNotThrow(() -> group.unsubscribe(sub1, "{\"unsub\":\"1\"}"));
    }

    @Test
    void testIdleConnectionReuseAndTimeoutClose() throws InterruptedException {
        GroupKey groupKey = GroupKey.of(MarketType.SPOT, Type.TRADES);
        Duration idleTimeout = Duration.ofMillis(100);
        ConnectionGroup group = new ConnectionGroup(groupKey, () -> createTestConnection(2), idleTimeout);

        SubscriptionKey sub1 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        group.subscribe(sub1, "{\"sub\":\"1\"}");

        ExchangeConnection conn = createdConnections.getFirst();
        assertEquals(1, group.physicalConnectionCount());
        assertEquals(0, group.getIdleConnections().size());

        group.unsubscribe(sub1, "{\"unsub\":\"1\"}");

        assertEquals(0, group.subscriptionCount());
        assertEquals(1, group.getIdleConnections().size());
        assertTrue(group.getIdleConnections().contains(conn));
        assertFalse(conn.isClosed());

        SubscriptionKey sub2 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("ETH-USDT"), new Command.Trades());
        group.subscribe(sub2, "{\"sub\":\"2\"}");

        assertEquals(1, group.subscriptionCount());
        assertEquals(0, group.getIdleConnections().size());
        assertEquals(1, createdConnections.size());
        assertSame(conn, group.getAssignments().get(sub2));

        group.unsubscribe(sub2, "{\"unsub\":\"2\"}");
        assertEquals(1, group.getIdleConnections().size());

        Thread.sleep(200);

        assertEquals(0, group.getIdleConnections().size());
        assertEquals(0, group.physicalConnectionCount());
        assertTrue(conn.isClosed());
    }

    @Test
    void testShutdownClosesAllConnections() {
        GroupKey groupKey = GroupKey.of(MarketType.SPOT, Type.TRADES);
        ConnectionGroup group = new ConnectionGroup(groupKey, () -> createTestConnection(2));

        SubscriptionKey sub1 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        group.subscribe(sub1, "{\"sub\":\"1\"}");

        ExchangeConnection conn = createdConnections.getFirst();
        assertFalse(conn.isClosed());

        group.shutdown();

        assertEquals(0, group.subscriptionCount());
        assertEquals(0, group.physicalConnectionCount());
        assertTrue(conn.isClosed());
    }
}
