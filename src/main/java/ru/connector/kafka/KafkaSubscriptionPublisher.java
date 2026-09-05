package ru.connector.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import ru.connector.api.dto.SubscriptionDto;

@Component
public class KafkaSubscriptionPublisher {

    private final KafkaSender<Long, SubscriptionDto> sender;
    private final String topic;

    public KafkaSubscriptionPublisher(
            KafkaSender<Long, SubscriptionDto> sender,
            @Value("${kafka.topic.subscriptions:market.subscriptions}") String topic) {
        this.sender = sender;
        this.topic = topic;
    }

    public Mono<Void> publish(Long id, SubscriptionDto request) {
        ProducerRecord<Long, SubscriptionDto> record = new ProducerRecord<>(topic, id, request);
        return sender.send(Mono.just(SenderRecord.create(record, id)))
                      .flatMap(res -> res.exception() != null ? Mono.error(res.exception()) : Mono.empty())
                      .then();
    }

    public Mono<Void> publishTombstone(Long id) {
        ProducerRecord<Long, SubscriptionDto> record = new ProducerRecord<>(topic, id, null);
        return sender.send(Mono.just(SenderRecord.create(record, id)))
                .flatMap(res -> res.exception() != null ? Mono.error(res.exception()) : Mono.empty())
                .then();
    }
}