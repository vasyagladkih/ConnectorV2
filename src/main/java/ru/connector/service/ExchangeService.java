package ru.connector.service;

import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.exceptions.NotFoundExchangeException;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.ExchangeManager;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.models.StreamKey;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static ru.connector.api.dto.SubscriptionResponse.toResponse;

@Service
public class ExchangeService {

    private final KafkaSubscriptionPublisher publisher;
    private final Map<String, ExchangeManager> managers;
    private final Scheduler singleScheduler = Schedulers.newSingle("sub-service");
    private final AtomicLong idSequence = new AtomicLong(0);

    public ExchangeService(
            KafkaSubscriptionPublisher publisher,
            Map<String, ExchangeManager> managers) {
        this.publisher = publisher;
        this.managers = managers;
    }

    public Mono<SubscriptionResponse> subscribe(Mono<SubscriptionDto> requestMono) {
        return requestMono
                .publishOn(singleScheduler)
                .flatMap(request -> {
                    ExchangeManager manager = Optional.ofNullable(managers.get(request.exchange().toUpperCase()))
                            .orElseThrow(() -> new NotFoundExchangeException("Exchange not supported: " + request.exchange()));

                    StreamKey key = StreamKey.from(request);

                    if (manager.exists(key)) {
                        Long existingId = manager.findIdByStreamKey(key)
                                .orElseThrow(() -> new IllegalStateException("StreamKey exists but id not found in manager"));
                        return Mono.just(toResponse(existingId, request));
                    }

                    Long id = idSequence.incrementAndGet();

                    return publisher.publish(id, request)
                            .then(Mono.defer(() -> manager.subscribe(id, request)
                                    .thenReturn(toResponse(id, request))
                                    .onErrorResume(error -> rollback(id, manager, error))));
                });
    }

    public Mono<Void> unsubscribe(Long id) {
        return Mono.defer(() -> {
            ExchangeManager manager = findManagerById(id)
                    .orElseThrow(() -> new SubscriptionNotFoundException(id));

            return publisher.publishTombstone(id)
                    .then(manager.unsubscribe(id));
        }).subscribeOn(singleScheduler);
    }

    public Flux<SubscriptionResponse> activeSubscriptions() {
        return Flux.defer(() -> Flux.fromIterable(managers.values())
                .flatMapIterable(ExchangeManager::getAllActive)
        ).subscribeOn(singleScheduler);
    }

    private Optional<ExchangeManager> findManagerById(Long id) {
        return managers.values().stream()
                .filter(manager -> manager.contains(id))
                .findFirst();
    }

    private Mono<SubscriptionResponse> rollback(Long id, ExchangeManager manager, Throwable error) {
        return publisher.publishTombstone(id)
                .then(manager.unsubscribe(id).onErrorResume(_ -> Mono.empty()))
                .then(Mono.error(error));
    }

    @PreDestroy
    public void shutdown() {singleScheduler.dispose();}
}