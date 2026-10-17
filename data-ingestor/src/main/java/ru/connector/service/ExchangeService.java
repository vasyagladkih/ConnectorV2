package ru.connector.service;

import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.connector.exceptions.NotFoundExchangeException;
import ru.connector.exchange.ExchangeManager;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.mapper.IngestorCommandMapper;
import ru.connector.models.SubscriptionDto;
import ru.datacollector.common.mapper.SubscriptionResponseMapper;
import ru.datacollector.common.model.CreateSubscriptionRequest;
import ru.datacollector.common.model.StreamKey;
import ru.datacollector.common.model.SubscriptionResponse;
import ru.datacollector.common.model.SubscriptionStatus;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ROOT;

@Service
public class ExchangeService {

    private final KafkaSubscriptionPublisher publisher;
    private final Map<String, ExchangeManager> managers;
    private final IngestorCommandMapper commandMapper;
    private final SubscriptionResponseMapper responseMapper;

    public ExchangeService(
            KafkaSubscriptionPublisher publisher,
            Map<String, ExchangeManager> managers,
            IngestorCommandMapper commandMapper,
            SubscriptionResponseMapper responseMapper) {
        this.publisher = publisher;
        this.managers = managers;
        this.commandMapper = commandMapper;
        this.responseMapper = responseMapper;
    }

    private SubscriptionResponse toResponse(String keyStr, CreateSubscriptionRequest request) {
        long id = (long) (keyStr.hashCode() & 0x7FFFFFFF);
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime expiresAt = request.durationMinutes() != null ? now.plusMinutes(request.durationMinutes()) : null;
        return responseMapper.toResponse(request, id, SubscriptionStatus.ACTIVE, now, expiresAt);
    }

    @Deprecated
    public Mono<SubscriptionResponse> saveToJournal(Mono<CreateSubscriptionRequest> requestMono) {
        return requestMono
                .flatMap(request -> {
                    getManager(request.exchange());
                    SubscriptionDto dto = commandMapper.toDto(request);
                    StreamKey key = dto.key();
                    String keyStr = key.toString();
                    return publisher.publish(keyStr, request)
                            .thenReturn(toResponse(keyStr, request));
                });
    }

    @Deprecated
    public Mono<Void> saveToJournalTombstone(String keyStr) {
        return Mono.defer(() -> {
            StreamKey key = StreamKey.parse(keyStr);
            getManager(key.exchange());
            return publisher.publishTombstone(keyStr);
        });
    }

    public Mono<Void> subscribe(CreateSubscriptionRequest request) {
        return subscribe(commandMapper.toDto(request));
    }

    public Mono<Void> subscribe(SubscriptionDto dto) {
        return Mono.defer(() -> {
            ExchangeManager manager = getManager(dto.exchange());
            StreamKey key = dto.key();
            if (manager.exists(key)) return Mono.empty();
            return manager.subscribe(dto);
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
                .map(dto -> {
                    CreateSubscriptionRequest request = commandMapper.toRequest(dto);
                    return toResponse(dto.key().toString(), request);
                })
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