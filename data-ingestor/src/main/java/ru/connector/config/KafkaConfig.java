package ru.connector.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import ru.connector.api.dto.SubscriptionDto;

import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic subscriptionStateTopic(@Value("${kafka.topics.subscription-state:market.subscriptions}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic rawDataTopic(@Value("${kafka.topics.raw:market.data.raw}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(12)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaTemplate<String, byte[]> rawDataKafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        Map<String, Object> configs = producerFactory.getConfigurationProperties();
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new ByteArraySerializer()));
    }

    @Bean
    public KafkaTemplate<String, SubscriptionDto> subscriptionKafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        Map<String, Object> configs = producerFactory.getConfigurationProperties();
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new JacksonJsonSerializer<SubscriptionDto>()));
    }
}