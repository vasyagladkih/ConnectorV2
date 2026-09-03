package ru.connector.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.connector.api.dto.Request;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.exceptions.NotFoundExchangeException;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.ExchangeManager;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.models.ClientSubscription;
import ru.connector.models.SubscriptionKey;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ExchangeService {

    private final Map<String, ExchangeManager> managers;
    private final KafkaSubscriptionPublisher subscriptionPublisher;

    private final AtomicLong idSequence = new AtomicLong(0);
    private final Map<Long, ClientSubscription> subscriptionsById = new ConcurrentHashMap<>();
    private final Map<SubscriptionKey, Set<Long>> subscribersByKey = new ConcurrentHashMap<>();

    public ExchangeService(Map<String, ExchangeManager> managers) {
        this(managers, null);
    }

    @Autowired
    public ExchangeService(
            Map<String, ExchangeManager> managers,
            @Autowired(required = false) KafkaSubscriptionPublisher subscriptionPublisher) {
        this.managers = managers;
        this.subscriptionPublisher = subscriptionPublisher;
    }

    public Mono<SubscriptionResponse> subscribe(Mono<Request> requestMono) {

        return requestMono.flatMap(req -> {
            ExchangeManager manager = Optional.ofNullable(managers.get(req.exchange().toUpperCase()))
                    .orElseThrow(() -> new NotFoundExchangeException("Exchange not supported: " + req.exchange()));

            SubscriptionKey key = SubscriptionKey.of(req.market(), req.symbol(), req.command());
            long id = idSequence.incrementAndGet();
            ClientSubscription clientSub = new ClientSubscription(id, req.exchange(), key, req.command(), Instant.now());
            subscriptionsById.put(id, clientSub);

            AtomicBoolean isFirstSubscriber = new AtomicBoolean(false);
            subscribersByKey.compute(key, (k, existingSet) -> {
                if (existingSet == null || existingSet.isEmpty()) {
                    isFirstSubscriber.set(true);
                    Set<Long> set = ConcurrentHashMap.newKeySet();
                    set.add(id);
                    return set;
                } else {
                    existingSet.add(id);
                    return existingSet;
                }
            });

            Mono<Void> physicalSubscribe = isFirstSubscriber.get()
                    ? Mono.fromRunnable(() -> manager.subscribe(req))
                    : Mono.empty();

            Mono<Void> kafkaPublish = Optional.ofNullable(subscriptionPublisher)
                    .map(p -> p.publish(id, req))
                    .orElseGet(Mono::empty);

            return physicalSubscribe
                    .then(kafkaPublish)
                    .thenReturn(new SubscriptionResponse(id, req.exchange(), req.market(), req.symbol(), req.command()));
        });
    }

    public Mono<Void> unsubscribe(Long id) {
        return Mono.defer(() -> {
            ClientSubscription clientSub = Optional.ofNullable(subscriptionsById.remove(id))
                    .orElseThrow(() -> new SubscriptionNotFoundException(id));

            SubscriptionKey key = clientSub.key();
            AtomicBoolean isLastSubscriber = new AtomicBoolean(false);

            subscribersByKey.computeIfPresent(key, (k, existingSet) -> {
                existingSet.remove(id);
                if (existingSet.isEmpty()) {
                    isLastSubscriber.set(true);
                    return null;
                }
                return existingSet;
            });

            Mono<Void> physicalUnsubscribe = isLastSubscriber.get()
                    ? Mono.fromRunnable(() -> {
                        Optional.ofNullable(managers.get(clientSub.exchange().toUpperCase()))
                                .ifPresent(mgr -> mgr.unsubscribe(new Request(
                                        clientSub.exchange(),
                                        key.market(),
                                        key.symbol(),
                                        clientSub.command()
                                )));
                    })
                    : Mono.empty();

            Mono<Void> kafkaPublish = Optional.ofNullable(subscriptionPublisher)
                    .map(p -> p.publishTombstone(id))
                    .orElseGet(Mono::empty);

            return physicalUnsubscribe.then(kafkaPublish);
        });
    }

    public Flux<SubscriptionResponse> activeSubscriptions() {
        return Flux.fromIterable(subscriptionsById.values())
                .map(sub -> new SubscriptionResponse(
                        sub.id(),
                        sub.exchange(),
                        sub.key().market(),
                        sub.key().symbol(),
                        sub.command()
                ));
    }
}
