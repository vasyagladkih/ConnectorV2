package ru.connector.kafka;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.service.ExchangeService;

import java.time.Duration;

@AllArgsConstructor
@Component
public class SubscriptionConsumer {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionConsumer.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final ExchangeService service;

    @KafkaListener(
            topics = "${kafka.topics.subscription-state:market.subscriptions}",
            groupId = "connector-subscription-group"
    )
    public void onMessage(ConsumerRecord<String, SubscriptionDto> consumerRecord, Acknowledgment ack) {
        String key = consumerRecord.key();
        SubscriptionDto request = consumerRecord.value();

        try {
            if (request != null) {
                service.subscribe(request).block(TIMEOUT);
            } else if (key != null) {
                service.unsubscribe(key).block(TIMEOUT);
            }
        } catch (Exception e) {
            log.error("Failed to process subscription record for key={}", key, e);
        } finally {
            ack.acknowledge();
        }
    }
}

