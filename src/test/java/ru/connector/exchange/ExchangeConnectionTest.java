package ru.connector.exchange;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.SubscriptionKey;
import ru.connector.models.Symbol;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ExchangeConnectionTest {

    private WebSocketClient mockClient;
    private KafkaRawDataPublisher mockPublisher;

    @BeforeEach
    void setUp() {
        mockClient = mock(WebSocketClient.class);
        mockPublisher = mock(KafkaRawDataPublisher.class);
    }

    @Test
    void testCapacityAndSubscriptionTracking() {
        ExchangeConnection conn = new ExchangeConnection(
                "TEST",
                MarketType.SPOT,
                "ws://localhost",
                mockClient,
                mockPublisher,
                "TEST_TOPIC",
                Duration.ofSeconds(10),
                2
        );

        assertTrue(conn.hasCapacity());
        assertEquals(0, conn.subscriptionCount());
        assertEquals(2, conn.getMaxSubscriptions());

        SubscriptionKey sub1 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        conn.subscribe(sub1, "{\"payload\":1}");

        assertEquals(1, conn.subscriptionCount());
        assertTrue(conn.hasCapacity());
        assertTrue(conn.getActiveSubscriptions().containsKey(sub1));

        SubscriptionKey sub2 = SubscriptionKey.of(MarketType.SPOT, Symbol.parse("ETH-USDT"), new Command.Trades());
        conn.subscribe(sub2, "{\"payload\":2}");

        assertEquals(2, conn.subscriptionCount());
        assertFalse(conn.hasCapacity());

        conn.unsubscribe(sub1, "{\"unsub\":1}");
        assertEquals(1, conn.subscriptionCount());
        assertTrue(conn.hasCapacity());
        assertFalse(conn.getActiveSubscriptions().containsKey(sub1));
        assertTrue(conn.getActiveSubscriptions().containsKey(sub2));
    }

    @Test
    void testDataBufferIsReleasedToPreventDirectMemoryLeak() {
        UnpooledByteBufAllocator nettyAllocator = new UnpooledByteBufAllocator(false);
        NettyDataBufferFactory factory = new NettyDataBufferFactory(nettyAllocator);

        byte[] rawContent = "{\"type\":\"message\",\"data\":\"market_tick\"}".getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = factory.wrap(rawContent);

        ByteBuf nativeByteBuf = ((org.springframework.core.io.buffer.NettyDataBuffer) buffer).getNativeBuffer();
        assertEquals(1, nativeByteBuf.refCnt());

        WebSocketMessage wsMessage = new WebSocketMessage(WebSocketMessage.Type.TEXT, buffer);

        byte[] resultBytes = ExchangeConnection.toRawBytes(wsMessage);

        assertArrayEquals(rawContent, resultBytes);
        assertEquals(0, nativeByteBuf.refCnt());
    }

    @Test
    void testShutdownMarksConnectionClosed() {
        ExchangeConnection conn = new ExchangeConnection(
                "TEST",
                MarketType.SPOT,
                "ws://localhost",
                mockClient,
                mockPublisher,
                "TEST_TOPIC",
                Duration.ofSeconds(10),
                10
        );

        assertFalse(conn.isClosed());
        conn.shutdown();
        assertTrue(conn.isClosed());

        assertThrows(IllegalStateException.class, conn::start);
    }

    @Test
    void testSendDelayMsConfigured() {
        ExchangeConnection conn = new ExchangeConnection(
                "TEST",
                MarketType.SPOT,
                "ws://localhost",
                mockClient,
                mockPublisher,
                "TEST_TOPIC",
                Duration.ofSeconds(10),
                10,
                75L
        );
        assertEquals(75L, conn.getSendDelayMs());
    }
}
