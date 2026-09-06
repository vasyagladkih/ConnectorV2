package ru.connector.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;

@Component
public class KafkaSubscriptionPublisher {

    private final KafkaTemplate<Long, SubscriptionDto> kafkaTemplate;
    private final String topic;

    public KafkaSubscriptionPublisher(
            KafkaTemplate<Long, SubscriptionDto> kafkaTemplate,
            @Value("${kafka.topics.subscription-state:market.subscriptions}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public Mono<Void> publish(Long id, SubscriptionDto request) {
        return Mono.fromFuture(() -> kafkaTemplate.send(topic, id, request)).then();
    }

    public Mono<Void> publishTombstone(Long id) {
        return Mono.fromFuture(() -> kafkaTemplate.send(topic, id, null)).then();
    }
}