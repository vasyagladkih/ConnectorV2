package ru.connector.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import ru.connector.api.dto.SubscriptionDto;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.acks:all}")
    private String acks;

    @Value("${spring.kafka.producer.batch-size:32768}")
    private String batchSize;

    @Value("${spring.kafka.producer.compression-type:lz4}")
    private String compressionType;

    @Value("${kafka.topics.subscription-state:market.subscriptions}")
    private String subscriptionTopic;

    @Value("${kafka.topics.raw:market.data.raw}")
    private String rawDataTopic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic subscriptionStateTopic() {
        return TopicBuilder.name(subscriptionTopic)
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic rawDataTopic() {
        return TopicBuilder.name(rawDataTopic)
                .partitions(12)
                .replicas(1)
                .build();
    }

    private Map<String, Object> baseProducerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }

    @Bean
    public KafkaTemplate<String, byte[]> rawDataKafkaTemplate() {
        Map<String, Object> props = baseProducerConfigs();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public KafkaTemplate<Long, SubscriptionDto> subscriptionKafkaTemplate(ObjectMapper objectMapper) {
        Map<String, Object> props = baseProducerConfigs();
        Serializer<SubscriptionDto> valueSerializer = (_, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Failed to serialize SubscriptionDto", e);
            }
        };

        DefaultKafkaProducerFactory<Long, SubscriptionDto> factory = new DefaultKafkaProducerFactory<>(
                props,
                new LongSerializer(),
                valueSerializer
        );
        return new KafkaTemplate<>(factory);
    }
}