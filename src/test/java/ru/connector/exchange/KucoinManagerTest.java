package ru.connector.exchange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.api.dto.Request;
import ru.connector.exchange.impl.kucoin.KucoinManager;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.Command;
import ru.connector.models.GroupKey;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;
import ru.connector.models.Type;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class KucoinManagerTest {

    private KucoinManager kucoinManager;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        WebSocketClient mockClient = mock(WebSocketClient.class);
        KafkaRawDataPublisher mockPublisher = mock(KafkaRawDataPublisher.class);
        kucoinManager = new KucoinManager(mockClient, mockPublisher, 2, Duration.ofSeconds(60));
        mapper = new ObjectMapper();
    }

    @Test
    void testDifferentGroupsDoNotShareConnections() {
        Request req1 = new Request(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("BTC-USDT"),
                new Command.Trades()
        );

        Request req2 = new Request(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("BTC-USDT"),
                new Command.OrderBook(5)
        );

        Request req3 = new Request(
                "KUCOIN",
                MarketType.FUTURES,
                Symbol.parse("BTC-USDT"),
                new Command.Trades()
        );

        kucoinManager.subscribe(req1);
        kucoinManager.subscribe(req2);
        kucoinManager.subscribe(req3);

        assertEquals(3, kucoinManager.getGroups().size());

        GroupKey spotTradesKey = GroupKey.of(MarketType.SPOT, Type.TRADES);
        GroupKey spotOrderBookKey = GroupKey.of(MarketType.SPOT, Type.ORDER_BOOK);
        GroupKey futuresTradesKey = GroupKey.of(MarketType.FUTURES, Type.TRADES);

        assertTrue(kucoinManager.getGroups().containsKey(spotTradesKey));
        assertTrue(kucoinManager.getGroups().containsKey(spotOrderBookKey));
        assertTrue(kucoinManager.getGroups().containsKey(futuresTradesKey));

        ConnectionGroup spotTradesGroup = kucoinManager.getGroups().get(spotTradesKey);
        ConnectionGroup spotOrderBookGroup = kucoinManager.getGroups().get(spotOrderBookKey);
        ConnectionGroup futuresTradesGroup = kucoinManager.getGroups().get(futuresTradesKey);

        assertEquals(1, spotTradesGroup.subscriptionCount());
        assertEquals(1, spotOrderBookGroup.subscriptionCount());
        assertEquals(1, futuresTradesGroup.subscriptionCount());

        assertNotSame(
                spotTradesGroup.getActiveConnections().iterator().next(),
                spotOrderBookGroup.getActiveConnections().iterator().next()
        );
    }

    @Test
    void testKucoinWireMessageTranslation() throws Exception {
        Request tradeReq = new Request(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("BTC-USDT"),
                new Command.Trades()
        );
        String tradeJson = kucoinManager.translate(tradeReq);
        JsonNode tradeNode = mapper.readTree(tradeJson);

        assertEquals("SUBSCRIBE", tradeNode.get("action").asText());
        assertEquals("trade", tradeNode.get("channel").asText());
        assertEquals("SPOT", tradeNode.get("tradeType").asText());
        assertEquals("BTC-USDT", tradeNode.get("symbol").asText());

        Request tickerReq = new Request(
                "KUCOIN",
                MarketType.FUTURES,
                Symbol.parse("BTC-USDT"),
                new Command.BookTicker()
        );
        String tickerJson = kucoinManager.translate(tickerReq);
        JsonNode tickerNode = mapper.readTree(tickerJson);

        assertEquals("SUBSCRIBE", tickerNode.get("action").asText());
        assertEquals("ticker", tickerNode.get("channel").asText());
        assertEquals("FUTURES", tickerNode.get("tradeType").asText());
        assertEquals("XBTUSDTM", tickerNode.get("symbol").asText());

        Request obReq = new Request(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("ETH-USDT"),
                new Command.OrderBook(5)
        );
        String obJson = kucoinManager.translate(obReq);
        JsonNode obNode = mapper.readTree(obJson);

        assertEquals("SUBSCRIBE", obNode.get("action").asText());
        assertEquals("obu", obNode.get("channel").asText());
        assertEquals("SPOT", obNode.get("tradeType").asText());
        assertEquals("ETH-USDT", obNode.get("symbol").asText());
        assertEquals("5", obNode.get("depth").asText());
        assertEquals(0, obNode.get("rpiFilter").asInt());
    }

    @Test
    void testManagerShutdownClearsAllGroups() {
        Request req = new Request(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("BTC-USDT"),
                new Command.Trades()
        );
        kucoinManager.subscribe(req);
        assertEquals(1, kucoinManager.getGroups().size());

        kucoinManager.shutdown();
        assertEquals(0, kucoinManager.getGroups().size());
    }
}
