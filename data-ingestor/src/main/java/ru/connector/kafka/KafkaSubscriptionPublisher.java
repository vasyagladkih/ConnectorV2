package ru.connector.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import ru.datacollector.common.model.CreateSubscriptionRequest;

@Deprecated
@Component
public class KafkaSubscriptionPublisher {

    private final KafkaTemplate<String, CreateSubscriptionRequest> kafkaTemplate;
    private final String topic;

    public KafkaSubscriptionPublisher(
            KafkaTemplate<String, CreateSubscriptionRequest> kafkaTemplate,
            @Value("${kafka.topics.subscription-state:market.subscriptions}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public Mono<Void> publish(String key, CreateSubscriptionRequest request) {
        return Mono.fromFuture(() -> kafkaTemplate.send(topic, key, request)).then();
    }

    public Mono<Void> publishTombstone(String key) {
        return Mono.fromFuture(() -> kafkaTemplate.send(topic, key, null)).then();
    }
}