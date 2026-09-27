package ru.connector.exchange;

import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.models.StreamKey;

import java.util.List;
import java.util.Optional;

@SuppressWarnings("unused")
public interface ExchangeManager {
    boolean exists(StreamKey key);
    Optional<SubscriptionDto> findRequest(StreamKey key);
    Mono<Void> subscribe(SubscriptionDto request);
    Mono<Void> unsubscribe(StreamKey key);
    List<SubscriptionResponse> getAllActive();
    void shutdown();
}