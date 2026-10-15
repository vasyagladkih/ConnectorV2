package ru.connector.service;

import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.exceptions.NotFoundExchangeException;
import ru.connector.exchange.ExchangeManager;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.models.StreamKey;

import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ROOT;
import static ru.connector.api.dto.SubscriptionResponse.toResponse;

@Service
public class ExchangeService {

    private final KafkaSubscriptionPublisher publisher;
    private final Map<String, ExchangeManager> managers;

    public ExchangeService(
            KafkaSubscriptionPublisher publisher,
            Map<String, ExchangeManager> managers) {
        this.publisher = publisher;
        this.managers = managers;
    }

    public Mono<SubscriptionResponse> saveToJournal(Mono<SubscriptionDto> requestMono) {
        return requestMono
                .flatMap(request -> {
                    getManager(request.exchange());
                    StreamKey key = StreamKey.from(request);
                    String keyStr = key.toString();
                    return publisher.publish(keyStr, request)
                            .thenReturn(toResponse(keyStr, request));
                });
    }

    public Mono<Void> saveToJournalTombstone(String keyStr) {
        return Mono.defer(() -> {
            StreamKey key = StreamKey.parse(keyStr);
            getManager(key.exchange());
            return publisher.publishTombstone(keyStr);
        });
    }

    public Mono<Void> subscribe(SubscriptionDto request) {
        return Mono.defer(() -> {
            ExchangeManager manager = getManager(request.exchange());
            StreamKey key = StreamKey.from(request);
            if (manager.exists(key)) return Mono.empty();
            return manager.subscribe(request);
        });
    }

    public Mono<Void> unsubscribe(String keyStr) {
        return Mono.defer(() -> {
            StreamKey key = StreamKey.parse(keyStr);
            ExchangeManager manager = getManager(key.exchange());
            return manager.unsubscribe(key);
        });
    }

    public Flux<SubscriptionResponse> activeSubscriptions() {
        return Flux.defer(() -> Flux.fromIterable(managers.values())
                .flatMapIterable(ExchangeManager::getAllActive)
        );
    }

    private @NonNull ExchangeManager getManager(String exchange) {
        return Optional.ofNullable(managers.get(exchange.toUpperCase(ROOT)))
                .orElseThrow(() -> new NotFoundExchangeException("Exchange not supported: " + exchange));
    }

    @PreDestroy
    public void shutdown() {
        managers.values().forEach(ExchangeManager::shutdown);
    }
}