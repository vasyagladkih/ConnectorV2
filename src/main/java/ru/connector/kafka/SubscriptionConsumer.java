package ru.connector.kafka;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.service.ExchangeService;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@AllArgsConstructor
@Component
public class SubscriptionConsumer implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionConsumer.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final ExchangeService service;

    @KafkaListener(
            topics = "${kafka.topics.subscription-state:market.subscriptions}",
            groupId = "connector-subscription-group"
    )
    public void onMessage(ConsumerRecord<String, SubscriptionDto> record) {
        try {
            Optional.ofNullable(record.value())
                    .map(service::subscribe)
                    .or(() -> Optional.ofNullable(record.key()).map(service::unsubscribe))
                    .ifPresent(action -> action.block(TIMEOUT));
        } catch (Exception e) {
            log.error("Failed to process subscription record for key={}", record.key(), e);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.info("Назначены партиции {}. Сброс смещения на начало топика для восстановления WAL...", assignments.keySet());
        callback.seekToBeginning(assignments.keySet());
    }
}

