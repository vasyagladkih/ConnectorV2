package ru.connector.transport;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;

/**
 * Единый (общий на все WS-потоки) отправитель сырых сообщений в Kafka.
 *
 * <p>Является «глупой трубой»: принимает уже готовый массив байтов (raw JSON с биржи)
 * и кладёт его в Kafka без какой-либо десериализации/нормализации содержимого.</p>
 */
public class KafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaSender<byte[], byte[]> sender;
    private final String defaultTopic;

    public KafkaPublisher(KafkaSender<byte[], byte[]> sender, String defaultTopic) {
        this.sender = sender;
        this.defaultTopic = defaultTopic;
    }

    /**
     * Fire-and-forget отправка в Kafka.
     *
     * @param topic  топик; если {@code null} — используется defaultTopic
     * @param key    ключ партиционирования (raw bytes)
     * @param value  сырые байты сообщения (raw JSON с биржи)
     */
    public void publish(String topic, String key, byte[] value) {
        String targetTopic = topic != null && !topic.isBlank() ? topic : defaultTopic;

        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(targetTopic, key.getBytes(StandardCharsets.UTF_8), value);

        sender.send(Mono.just(SenderRecord.create(record, record.key())))
                .doOnNext(result -> {
                    // ack от брокера — здесь можно навесить метрики/логирование
                })
                .doOnError(e -> log.error("Kafka send failed for topic={}, key={}", targetTopic, key, e))
                .subscribe();
    }

    public void publish(String key, byte[] value) {
        publish(null, key, value);
    }

    public void close() {
        sender.close();
    }
}