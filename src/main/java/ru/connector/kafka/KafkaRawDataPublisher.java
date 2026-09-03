package ru.connector.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Sinks;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Component
public class KafkaRawDataPublisher {

    private final KafkaSender<byte[], byte[]> sender;
    private final String defaultTopic;
    private final Sinks.Many<SenderRecord<byte[], byte[], byte[]>> sink =
            Sinks.many().unicast().onBackpressureBuffer();
    private final Disposable deliverySubscription;

    public KafkaRawDataPublisher(
            KafkaSender<byte[], byte[]> sender,
            @Value("${kafka.topic.raw}") String defaultTopic) {
        this.sender = sender;
        this.defaultTopic = defaultTopic;
        this.deliverySubscription = this.sink.asFlux()
                .as(this.sender::send)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(500))
                        .maxBackoff(Duration.ofSeconds(10)))
                .subscribe();
    }

    public void publish(String topic, String key, byte[] value) {
        String targetTopic = topic != null && !topic.isBlank() ? topic : defaultTopic;
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(targetTopic, keyBytes, value);
        sink.tryEmitNext(SenderRecord.create(record, keyBytes));
    }

    public void publish(String key, byte[] value) {
        publish(null, key, value);
    }

    public void close() {
        if (deliverySubscription != null && !deliverySubscription.isDisposed()) {
            deliverySubscription.dispose();
        }
        sink.tryEmitComplete();
        sender.close();
    }
}