package ru.connector.exchange;

import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.models.StreamKey;

import java.util.List;
import java.util.Optional;

public interface ExchangeManager {
    boolean exists(StreamKey key);
    Optional<Long> findIdByStreamKey(StreamKey key);
    boolean contains(Long id);
    Optional<SubscriptionDto> findRequestById(Long id);
    Mono<Void> subscribe(Long id, SubscriptionDto request);
    Mono<Void> unsubscribe(Long id);
    List<SubscriptionResponse> getAllActive();
    void shutdown();
}