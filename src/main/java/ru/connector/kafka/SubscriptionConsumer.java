package ru.connector.kafka;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
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
    public void onMessage(ConsumerRecord<Object, SubscriptionDto> record, Acknowledgment ack) {
        SubscriptionDto request = record.value();

        try {
            if (request != null) {
                service.subscribe(Mono.just(request)).block(TIMEOUT);
            } else {
                Object keyObj = record.key();
                if (keyObj != null) {
                    try {
                        Long id = (keyObj instanceof Long l) ? l : Long.parseLong(keyObj.toString());
                        service.unsubscribe(id).block(TIMEOUT);
                    } catch (NumberFormatException e) {
                        log.error("Invalid key format for unsubscribe: {}", keyObj, e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to process subscription record for key={}", record.key(), e);
        } finally {
            ack.acknowledge();
        }
    }
}

