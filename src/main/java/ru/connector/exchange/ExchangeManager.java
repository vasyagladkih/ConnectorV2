package ru.connector.exchange;

import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.models.StreamKey;

import java.util.List;

public interface ExchangeManager {
    boolean tryReserve(StreamKey key, SubscriptionDto request);
    Mono<Void> subscribe(StreamKey key, SubscriptionDto request);
    Mono<Void> unsubscribe(StreamKey key);
    void rollback(StreamKey key);
    boolean exists(StreamKey key);
    List<SubscriptionResponse> getAllActive();
    void shutdown();
}
