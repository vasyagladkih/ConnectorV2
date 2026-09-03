package ru.connector.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import ru.connector.api.dto.Request;

@Component
public class KafkaSubscriptionPublisher {

    private final KafkaSender<String, Request> sender;
    private final String topic;

    public KafkaSubscriptionPublisher(
            KafkaSender<String, Request> sender,
            @Value("${kafka.topics.subscription-state:subscriptions-state}") String topic) {
        this.sender = sender;
        this.topic = topic;
    }

    public Mono<Void> publish(Long id, Request request) {
        String key = String.valueOf(id);
        ProducerRecord<String, Request> record = new ProducerRecord<>(topic, key, request);
        return sender.send(Mono.just(SenderRecord.create(record, key))).then();
    }

    public Mono<Void> publishTombstone(Long id) {
        String key = String.valueOf(id);
        ProducerRecord<String, Request> record = new ProducerRecord<>(topic, key, null);
        return sender.send(Mono.just(SenderRecord.create(record, key))).then();
    }
}