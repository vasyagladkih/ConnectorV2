package ru.connector.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import reactor.test.StepVerifier;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaSubscriptionPublisherTest {

    @Mock
    private KafkaTemplate<String, SubscriptionDto> kafkaTemplate;

    private KafkaSubscriptionPublisher publisher;
    private static final String TOPIC = "market.subscriptions";
    private static final String KEY = "KUCOIN:SPOT:BTC-USDT:TRADES";

    @BeforeEach
    void setUp() {
        publisher = new KafkaSubscriptionPublisher(kafkaTemplate, TOPIC);
    }

    @Test
    @DisplayName("publish: отправляет сообщение со строковым ключом (String) и SubscriptionDto в Kafka")
    void publishSendsStringKeyAndDto() {
        SubscriptionDto dto = new SubscriptionDto(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("BTC-USDT"),
                new Command.Trades()
        );

        CompletableFuture<SendResult<String, SubscriptionDto>> future = new CompletableFuture<>();
        future.complete(null);

        when(kafkaTemplate.send(eq(TOPIC), eq(KEY), eq(dto))).thenReturn(future);

        StepVerifier.create(publisher.publish(KEY, dto))
                .verifyComplete();

        verify(kafkaTemplate).send(TOPIC, KEY, dto);
    }

    @Test
    @DisplayName("publishTombstone: отправляет null value (tombstone) со строковым ключом (String)")
    void publishTombstoneSendsNullValue() {
        CompletableFuture<SendResult<String, SubscriptionDto>> future = new CompletableFuture<>();
        future.complete(null);

        when(kafkaTemplate.send(eq(TOPIC), eq(KEY), isNull())).thenReturn(future);

        StepVerifier.create(publisher.publishTombstone(KEY))
                .verifyComplete();

        verify(kafkaTemplate).send(TOPIC, KEY, null);
    }

    @Test
    @DisplayName("publish: пробрасывает ошибку при сбое отправки в Kafka")
    void publishPropagatesErrorOnFailure() {
        SubscriptionDto dto = new SubscriptionDto(
                "KUCOIN",
                MarketType.SPOT,
                Symbol.parse("BTC-USDT"),
                new Command.Trades()
        );

        CompletableFuture<SendResult<String, SubscriptionDto>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Kafka connection error"));

        when(kafkaTemplate.send(eq(TOPIC), eq(KEY), eq(dto))).thenReturn(future);

        StepVerifier.create(publisher.publish(KEY, dto))
                .expectErrorMessage("Kafka connection error")
                .verify();

        verify(kafkaTemplate).send(TOPIC, KEY, dto);
    }
}
