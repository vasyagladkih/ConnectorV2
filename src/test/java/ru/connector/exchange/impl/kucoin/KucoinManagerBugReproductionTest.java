package ru.connector.exchange.impl.kucoin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.exchange.registry.SubscriptionsRegistry;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.Command;
import ru.connector.models.GroupKey;
import ru.connector.models.MarketType;
import ru.connector.models.StreamKey;
import ru.connector.models.Symbol;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KucoinManagerBugReproductionTest {

    @Mock
    private WebSocketClient mockWsClient;

    @Mock
    private KafkaRawDataPublisher mockRawPublisher;

    private SubscriptionsRegistry registry;
    private KucoinManager kucoinManager;
    private JsonMapper jsonMapper;

    @BeforeEach
    void setUp() {
        registry = new SubscriptionsRegistry();
        jsonMapper = JsonMapper.builder().build();
        kucoinManager = new KucoinManager(registry, mockWsClient, mockRawPublisher, jsonMapper);
    }

    private WebSocketSession setupMockSession() {
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.close()).thenReturn(Mono.empty());
        when(session.receive()).thenReturn(Flux.never());
        when(session.textMessage(anyString())).thenAnswer(inv -> {
            WebSocketMessage msg = mock(WebSocketMessage.class);
            when(msg.getPayloadAsText()).thenReturn(inv.getArgument(0));
            return msg;
        });
        when(session.send(any())).thenAnswer(sendInv -> {
            Publisher<WebSocketMessage> publisher = sendInv.getArgument(0);
            return Flux.from(publisher).then();
        });
        return session;
    }

    @Test
    @DisplayName("При отмене подписки (cancel) слот должен освобождаться, а PENDING откатываться")
    void shouldReleaseSlotAndRollbackPendingOnCancellation() {
        when(mockWsClient.execute(any(URI.class), any(WebSocketHandler.class)))
                .thenReturn(Mono.never());

        SubscriptionDto request = new SubscriptionDto("KUCOIN", MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        StreamKey key = StreamKey.from(request);
        assertTrue(registry.tryReserve(key, request));

        StepVerifier.create(kucoinManager.subscribe(key, request))
                .thenCancel()
                .verify();

        GroupKey groupKey = GroupKey.of(request.market(), request.type());
        ExchangeConnection conn = registry.getOrCreateConnection(groupKey, () -> null);

        assertEquals(0, conn.getActiveSlots(),
                "activeSlots должен быть 0 после отмены подписки");
        assertFalse(registry.exists(key),
                "Ключ должен быть удален из реестра при отмене");
        assertTrue(registry.tryReserve(key, request),
                "Повторный tryReserve должен быть успешен");
    }

    @Test
    @DisplayName("Ключ партиционирования Kafka обязан содержать символ инструмента (EXCHANGE:MARKET:SYMBOL)")
    void shouldPartitionKafkaRecordsByExchangeMarketAndSymbol() {
        WebSocketSession session = setupMockSession();

        String tradePayload = "{\"T\":\"trade.BTC-USDT\",\"d\":{\"s\":\"BTC-USDT\",\"p\":\"65000\"}}";
        byte[] payloadBytes = tradePayload.getBytes(StandardCharsets.UTF_8);

        when(session.receive()).thenReturn(
                Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT,
                        new DefaultDataBufferFactory().wrap(payloadBytes)))
                        .concatWith(Flux.never())
        );

        when(mockWsClient.execute(any(URI.class), any(WebSocketHandler.class)))
                .thenAnswer(inv -> {
                    WebSocketHandler handler = inv.getArgument(1);
                    return handler.handle(session);
                });

        SubscriptionDto request = new SubscriptionDto("KUCOIN", MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        StreamKey key = StreamKey.from(request);
        registry.tryReserve(key, request);

        kucoinManager.subscribe(key, request).block();

        ArgumentCaptor<String> partitionKeyCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockRawPublisher, atLeastOnce()).publish(partitionKeyCaptor.capture(), any(byte[].class));

        String actualKey = partitionKeyCaptor.getValue();

        assertEquals("KUCOIN:SPOT:BTC-USDT", actualKey,
                "partitionKey обязан содержать символ инструмента");
    }

    @Test
    @DisplayName("Отписка во время PENDING не должна бросать IllegalStateException (HTTP 500)")
    void shouldHandleUnsubscribeCleanlyDuringPendingState() {
        SubscriptionDto request = new SubscriptionDto("KUCOIN", MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        StreamKey key = StreamKey.from(request);
        registry.tryReserve(key, request);

        assertDoesNotThrow(() -> kucoinManager.unsubscribe(key).block(),
                "unsubscribe во время PENDING не должен бросать исключение");
    }

    @Test
    @DisplayName("Повторная отписка не должна декрементировать чужие слоты и закрывать сокет")
    void shouldNotCloseSocketForOtherStreamsOnDuplicateUnsubscribe() {
        WebSocketSession session = setupMockSession();

        when(mockWsClient.execute(any(URI.class), any(WebSocketHandler.class)))
                .thenAnswer(inv -> {
                    WebSocketHandler handler = inv.getArgument(1);
                    return handler.handle(session);
                });

        SubscriptionDto btcSub = new SubscriptionDto("KUCOIN", MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());
        SubscriptionDto ethSub = new SubscriptionDto("KUCOIN", MarketType.SPOT, Symbol.parse("ETH-USDT"), new Command.Trades());
        StreamKey btcKey = StreamKey.from(btcSub);
        StreamKey ethKey = StreamKey.from(ethSub);

        registry.tryReserve(btcKey, btcSub);
        kucoinManager.subscribe(btcKey, btcSub).block();

        registry.tryReserve(ethKey, ethSub);
        kucoinManager.subscribe(ethKey, ethSub).block();

        GroupKey groupKey = GroupKey.of(MarketType.SPOT, ru.connector.models.Type.TRADES);
        ExchangeConnection conn = registry.getOrCreateConnection(groupKey, () -> null);

        kucoinManager.unsubscribe(btcKey).block();
        conn.releaseSlot(btcKey);

        assertFalse(conn.isClosed(),
                "Сокет не должен закрываться для других активных подписок");
        assertEquals(1, conn.getActiveSlots(),
                "Слот ETH-USDT должен оставаться активным");
    }
}
