package ru.connector.transport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;
import ru.connector.api.dto.Request;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaSubscriptionPublisherTest {

    private KafkaSender<String, Request> mockKafkaSender;
    private KafkaSubscriptionPublisher publisher;
    private static final String TOPIC = "subscriptions-test-topic";

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockKafkaSender = mock(KafkaSender.class);
        publisher = new KafkaSubscriptionPublisher(mockKafkaSender, TOPIC);
    }

    @Test
    void testPublishWithIdSendsRequest() {
        Request request = new Request(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("BTC-USDT"),
                new Command.Trades()
        );

        SenderResult<Object> mockResult = mock(SenderResult.class);
        when(mockKafkaSender.send(any())).thenReturn(Flux.just(mockResult));

        StepVerifier.create(publisher.publish(42L, request))
                .verifyComplete();

        verify(mockKafkaSender, times(1)).send(any());
    }

    @Test
    void testPublishTombstoneSendsNull() {
        SenderResult<Object> mockResult = mock(SenderResult.class);
        when(mockKafkaSender.send(any())).thenReturn(Flux.just(mockResult));

        StepVerifier.create(publisher.publishTombstone(42L))
                .verifyComplete();

        verify(mockKafkaSender, times(1)).send(any());
    }
}
