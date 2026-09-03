package ru.connector.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import ru.connector.api.dto.Request;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

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

        return KafkaSender.create(SenderOptions.create(props));
    }

    @Bean
    @SuppressWarnings("removal")
    public KafkaSender<String, Request> subscriptionKafkaSender(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, acks);

        SenderOptions<String, Request> senderOptions = SenderOptions.<String, Request>create(props)
                .withKeySerializer(new StringSerializer())
                .withValueSerializer(new JsonSerializer<>(objectMapper));

        return KafkaSender.create(senderOptions);
    }
}