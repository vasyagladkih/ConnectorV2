package ru.connector.transport;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.raw}")
    private String rawTopic;

    @Value("${kafka.producer.acks:all}")
    private String acks;

    @Value("${kafka.producer.linger-ms:5}")
    private String lingerMs;

    @Value("${kafka.producer.batch-size:16384}")
    private String batchSize;

    @Bean
    public KafkaSender<byte[], byte[]> kafkaSender() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        log.info("Kafka sender configured for bootstrap-servers={}, raw-topic={}", bootstrapServers, rawTopic);

        return KafkaSender.create(SenderOptions.create(props));
    }

    @Bean
    public KafkaPublisher kafkaPublisher(KafkaSender<byte[], byte[]> sender) {
        return new KafkaPublisher(sender, rawTopic);
    }
}