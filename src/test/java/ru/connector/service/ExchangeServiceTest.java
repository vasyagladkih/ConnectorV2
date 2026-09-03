package ru.connector.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import ru.connector.api.dto.Request;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.ExchangeManager;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ExchangeServiceTest {

    private ExchangeManager mockManager;
    private KafkaSubscriptionPublisher mockPublisher;
    private ExchangeService exchangeService;

    @BeforeEach
    void setUp() {
        mockManager = mock(ExchangeManager.class);
        mockPublisher = mock(KafkaSubscriptionPublisher.class);
        when(mockPublisher.publish(any(), any())).thenReturn(Mono.empty());
        when(mockPublisher.publishTombstone(any())).thenReturn(Mono.empty());

        exchangeService = new ExchangeService(Map.of("KUCOIN", mockManager), mockPublisher);
    }

    @Test
    void testReferenceCountingTwoSubscribersOnePhysicalSubscribe() {
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
                new Command.Trades()
        );

        SubscriptionResponse resp1 = exchangeService.subscribe(Mono.just(req1)).block();
        SubscriptionResponse resp2 = exchangeService.subscribe(Mono.just(req2)).block();

        assertNotNull(resp1);
        assertNotNull(resp2);
        assertNotEquals(resp1.id(), resp2.id());

        verify(mockManager, times(1)).subscribe(any());
        verify(mockPublisher, times(1)).publish(eq(resp1.id()), any());
        verify(mockPublisher, times(1)).publish(eq(resp2.id()), any());

        exchangeService.unsubscribe(resp1.id()).block();
        verify(mockManager, never()).unsubscribe(any());
        verify(mockPublisher, times(1)).publishTombstone(eq(resp1.id()));

        exchangeService.unsubscribe(resp2.id()).block();
        verify(mockManager, times(1)).unsubscribe(any());
        verify(mockPublisher, times(1)).publishTombstone(eq(resp2.id()));
    }

    @Test
    void testUnsubscribeNonExistentIdThrowsException() {
        StepVerifier.create(exchangeService.unsubscribe(12345L))
                .expectError(SubscriptionNotFoundException.class)
                .verify();
    }

    @Test
    void testActiveSubscriptionsTracking() {
        Request req = new Request(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("ETH-USDT"),
                new Command.Trades()
        );

        SubscriptionResponse resp = exchangeService.subscribe(Mono.just(req)).block();
        assertNotNull(resp);

        StepVerifier.create(exchangeService.activeSubscriptions())
                .assertNext(sub -> {
                    assertEquals(resp.id(), sub.id());
                    assertEquals("ETH-USDT", sub.symbol().toString());
                })
                .verifyComplete();

        exchangeService.unsubscribe(resp.id()).block();

        StepVerifier.create(exchangeService.activeSubscriptions())
                .verifyComplete();
    }
}
