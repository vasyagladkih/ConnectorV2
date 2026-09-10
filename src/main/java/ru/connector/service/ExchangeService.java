package ru.connector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.exceptions.NotFoundExchangeException;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.ExchangeManager;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.models.StreamKey;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class ExchangeService {

    private static final Logger log = LoggerFactory.getLogger(ExchangeService.class);

    private final KafkaSubscriptionPublisher publisher;
    private final Map<String, ExchangeManager> managers;

    public ExchangeService(KafkaSubscriptionPublisher publisher, Map<String, ExchangeManager> managers) {
        this.publisher = publisher;
        this.managers = managers;
    }

    public Mono<SubscriptionResponse> subscribe(Mono<SubscriptionDto> requestMono) {
        return requestMono.flatMap(request -> {
            ExchangeManager manager = getManager(request.exchange());
            StreamKey key = StreamKey.from(request);
            String topicKey = key.toTopicKey(request.exchange());

            if (!manager.tryReserve(key, request)) {
                return Mono.just(SubscriptionResponse.toResponse(request));
            }

            return publisher.publish(topicKey, request)
                    .then(Mono.defer(() -> manager.subscribe(key, request)))
                    .thenReturn(SubscriptionResponse.toResponse(request))
                    .onErrorResume(error -> rollback(topicKey, key, manager, error));
        });
    }

    public Mono<Void> unsubscribe(SubscriptionDto request) {
        return Mono.defer(() -> {
            ExchangeManager manager = getManager(request.exchange());
            StreamKey key = StreamKey.from(request);
            String topicKey = key.toTopicKey(request.exchange());

            if (!manager.exists(key)) {
                return Mono.error(new SubscriptionNotFoundException("Subscription not found for stream: " + key));
            }

            return publisher.publishTombstone(topicKey)
                    .then(Mono.defer(() -> manager.unsubscribe(key)));
        });
    }

    public Flux<SubscriptionResponse> activeSubscriptions() {
        return Flux.defer(() -> Flux.fromIterable(managers.values())
                .flatMapIterable(ExchangeManager::getAllActive)
        );
    }

    private ExchangeManager getManager(String exchange) {
        return Optional.ofNullable(managers.get(exchange.toUpperCase()))
                .orElseThrow(() -> new NotFoundExchangeException("Exchange not supported: " + exchange));
    }

    private Mono<SubscriptionResponse> rollback(String topicKey, StreamKey key, ExchangeManager manager, Throwable error) {
        log.error("Failed to process subscription for stream [{}]: {}", key, error.getMessage(), error);
        manager.rollback(key);
        return publisher.publishTombstone(topicKey)
                .then(Mono.error(error));
    }

    public void shutdown() {
        managers.values().forEach(ExchangeManager::shutdown);
    }
}
