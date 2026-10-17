package ru.connector.kafka;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;
import ru.connector.service.ExchangeService;
import ru.datacollector.common.model.CreateSubscriptionRequest;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@Slf4j
@AllArgsConstructor
@Component
public class SubscriptionConsumer implements ConsumerSeekAware {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final ExchangeService service;

    @KafkaListener(
            topics = "${kafka.topics.subscription-state:market.subscriptions}",
            groupId = "connector-subscription-group"
    )
    public void onMessage(ConsumerRecord<String, CreateSubscriptionRequest> record) {
        try {
            if (record.value() != null) {
                log.info("[KAFKA] Получена команда ПОДПИСКИ: key={}", record.key());
            } else {
                log.info("[KAFKA] Получен TOMBSTONE (команда ОТПИСКИ): key={}", record.key());
            }

            Optional.ofNullable(record.value())
                    .map(service::subscribe)
                    .or(() -> Optional.ofNullable(record.key()).map(service::unsubscribe))
                    .ifPresent(action -> action.block(TIMEOUT));
        } catch (Exception e) {
            log.error("[KAFKA] Ошибка обработки записи для key={}", record.key(), e);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.info("Назначены партиции {}. Сброс смещения на начало топика для восстановления WAL...", assignments.keySet());
        callback.seekToBeginning(assignments.keySet());
    }
}

