package ru.connector.exchange.actor;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.network.ExchangeAdapter;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.GroupKey;
import ru.connector.models.StreamKey;
import ru.connector.transport.KucoinRegistry;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Реактивный актор управления пулом сокетов для заданной группы (GroupKey).
 * <p>
 * Реализует:
 * - Последовательную обработку подписок/отписок без блокировок
 * - Стратегию уплотнения (First-Fit) до лимита емкости
 * - Автоматическое удаление и закрытие пустых сокетов при 0 активных подписок
 * - Быстрые потокобезопасные запросы O(1) через ConcurrentHashMap
 */
public final class GroupPoolActor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GroupPoolActor.class);

    private final GroupKey groupKey;
    private final ExchangeAdapter adapter;
    private final WebSocketClient wsClient;
    private final KafkaRawDataPublisher rawPublisher;
    private final Function<SubscriptionDto, String> subscribeTranslator;
    private final Function<SubscriptionDto, String> unsubscribeTranslator;
    private final int maxSubscriptionsPerConnection;

    private final Sinks.Many<PoolCommand> mailbox = Sinks.many().unicast().onBackpressureBuffer();
    private final Scheduler scheduler;
    private final AtomicInteger connectionIdSequence = new AtomicInteger(0);

    // Внутреннее состояние актора (доступно только из потока scheduler)
    private final List<ConnectionActor> pool = new ArrayList<>();
    private final Map<Long, SubscriptionDto> idToRequest = new HashMap<>();
    private final Map<StreamKey, Long> streamToId = new HashMap<>();
    private final Map<Long, ConnectionActor> idToConnection = new HashMap<>();
    private final Map<ConnectionActor, Set<Long>> connectionToIds = new HashMap<>();

    private final Map<StreamKey, Long> readStreamToId = new ConcurrentHashMap<>();
    private final Map<Long, SubscriptionDto> readIdToRequest = new ConcurrentHashMap<>();

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private volatile boolean closed = false;
    private Disposable mailboxLoop;

    public GroupPoolActor(
            GroupKey groupKey,
            ExchangeAdapter adapter,
            WebSocketClient wsClient,
            KafkaRawDataPublisher rawPublisher,
            Function<SubscriptionDto, String> subscribeTranslator,
            Function<SubscriptionDto, String> unsubscribeTranslator
    ) {
        this.groupKey = groupKey;
        this.adapter = adapter;
        this.wsClient = wsClient;
        this.rawPublisher = rawPublisher;
        this.subscribeTranslator = subscribeTranslator;
        this.unsubscribeTranslator = unsubscribeTranslator;
        this.maxSubscriptionsPerConnection = adapter.getMaxSubscriptionsPerConnection();
        this.scheduler = Schedulers.newSingle("pool-actor-" + groupKey.market() + "-" + groupKey.type());
    }

    public void start() {
        if (closed || !isStarted.compareAndSet(false, true)) return;

        this.mailboxLoop = mailbox.asFlux()
                .publishOn(scheduler)
                .concatMap(this::handleCommand)
                .subscribe();
    }

    private Mono<Void> handleCommand(PoolCommand command) {
        return switch (command) {
            case PoolCommand.Subscribe sub -> handleSubscribe(sub);
            case PoolCommand.Unsubscribe unsub -> handleUnsubscribe(unsub);
            case PoolCommand.Close closeCmd -> handleClose(closeCmd);
        };
    }

    private Mono<Void> handleSubscribe(PoolCommand.Subscribe cmd) {
        Long id = cmd.id();
        SubscriptionDto request = cmd.request();
        StreamKey streamKey = StreamKey.from(request);

        if (streamToId.containsKey(streamKey)) {
            cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
            return Mono.empty();
        }

        ConnectionActor chosenConn = getChosenConn();

        String frame = subscribeTranslator.apply(request);

        return chosenConn.subscribe(streamKey, frame)
                .doOnSuccess(_ -> {
                    idToRequest.put(id, request);
                    streamToId.put(streamKey, id);
                    idToConnection.put(id, chosenConn);
                    connectionToIds.get(chosenConn).add(id);

                    readStreamToId.put(streamKey, id);
                    readIdToRequest.put(id, request);

                    cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
                })
                .doOnError(err -> {
                    log.error("Ошибка подписки на сокете {}", chosenConn.getId(), err);
                    Set<Long> ids = connectionToIds.get(chosenConn);
                    if (ids != null && ids.isEmpty()) {
                        chosenConn.close();
                        pool.remove(chosenConn);
                        connectionToIds.remove(chosenConn);
                    }
                    cmd.reply().emitError(err, Sinks.EmitFailureHandler.FAIL_FAST);
                });
    }

    private @NonNull ConnectionActor getChosenConn() {
        return pool.stream()
            .filter(c -> !c.isClosed())
            .filter(c -> {
                Set<Long> ids = connectionToIds.get(c);
                return ids != null && ids.size() < maxSubscriptionsPerConnection;
            })
            .findFirst()
            .orElseGet(() -> {
                var conn = createConnection();
                pool.add(conn);
                connectionToIds.put(conn, new HashSet<>());
                return conn;
            });
    }

    private Mono<Void> handleUnsubscribe(PoolCommand.Unsubscribe command) {
        Long id = command.id();
        SubscriptionDto request = idToRequest.get(id);
        if (request == null) {
            command.reply().emitError(new SubscriptionNotFoundException(id), Sinks.EmitFailureHandler.FAIL_FAST);
            return Mono.empty();
        }

        ConnectionActor conn = idToConnection.get(id);
        if (conn == null) {
            command.reply().emitError(new IllegalStateException("Не найдено соединение для подписки: " + id), Sinks.EmitFailureHandler.FAIL_FAST);
            return Mono.empty();
        }

        String unsubFrame = unsubscribeTranslator.apply(request);
        StreamKey streamKey = StreamKey.from(request);

        return conn.unsubscribe(streamKey, unsubFrame)
                .doOnSuccess(_ -> {
                    cleanupSubscription(id, streamKey, conn);
                    command.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
                })
                .doOnError(err -> {
                    cleanupSubscription(id, streamKey, conn);
                    command.reply().emitError(err, Sinks.EmitFailureHandler.FAIL_FAST);
                });
    }

    private void cleanupSubscription(Long id, StreamKey streamKey, ConnectionActor conn) {
        idToRequest.remove(id);
        streamToId.remove(streamKey);
        idToConnection.remove(id);

        readStreamToId.remove(streamKey);
        readIdToRequest.remove(id);

        Set<Long> ids = connectionToIds.get(conn);
        if (ids != null) {
            ids.remove(id);
            // Если на сокете не осталось активных подписок — закрываем и удаляем из пула
            if (ids.isEmpty()) {
                log.info("Соединение {} освободилось (0 подписок), закрываем сокет", conn.getId());
                conn.close();
                pool.remove(conn);
                connectionToIds.remove(conn);
            }
        }
    }

    private ConnectionActor createConnection() {
        int connId = connectionIdSequence.incrementAndGet();
        URI url = URI.create(KucoinRegistry.getWsUrl(groupKey.market()));
        String partitionKey = "KUCOIN:" + groupKey.market();

        ConnectionActor conn = ConnectionActor.builder()
                .id(connId)
                .exchangeUrl(url)
                .adapter(adapter)
                .wsClient(wsClient)
                .dataConsumer(bytes -> rawPublisher.publish(partitionKey, bytes))
                .fatalErrorConsumer(err -> log.error("Фатальная ошибка сокета {}", connId, err))
                .build();
        conn.start();
        return conn;
    }

    private Mono<Void> handleClose(PoolCommand.Close cmd) {
        close();
        cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        return Mono.empty();
    }

    /**
     * Отправить команду подписки в очередь пула.
     */
    public Mono<Void> subscribe(Long id, SubscriptionDto request) {
        if (closed) {
            return Mono.error(new IllegalStateException("Пул сокетов закрыт: " + groupKey));
        }
        Sinks.One<Void> reply = Sinks.one();
        Sinks.EmitResult result = mailbox.tryEmitNext(new PoolCommand.Subscribe(id, request, reply));
        if (result.isFailure()) {
            return Mono.error(new IllegalStateException("Очередь пула отклонила подписку: " + result));
        }
        return reply.asMono();
    }

    /**
     * Отправить команду отписки в очередь пула.
     */
    public Mono<Void> unsubscribe(Long id) {
        if (closed) return Mono.error(new IllegalStateException("Пул сокетов закрыт: " + groupKey));
        Sinks.One<Void> reply = Sinks.one();
        Sinks.EmitResult result = mailbox.tryEmitNext(new PoolCommand.Unsubscribe(id, reply));
        if (result.isFailure()) {
            return Mono.error(new IllegalStateException("Очередь пула отклонила отписку: " + result));
        }
        return reply.asMono();
    }

    public boolean exists(StreamKey key) {
        return readStreamToId.containsKey(key);
    }

    public Optional<Long> findIdByStreamKey(StreamKey key) {
        return Optional.ofNullable(readStreamToId.get(key));
    }

    public boolean contains(Long id) {
        return readIdToRequest.containsKey(id);
    }

    public Optional<SubscriptionDto> findRequestById(Long id) {
        return Optional.ofNullable(readIdToRequest.get(id));
    }

    public List<SubscriptionDto> getAllActive() {
        return List.copyOf(readIdToRequest.values());
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        pool.forEach(ConnectionActor::close);
        pool.clear();
        connectionToIds.clear();
        idToRequest.clear();
        streamToId.clear();
        idToConnection.clear();
        readStreamToId.clear();
        readIdToRequest.clear();
        if (mailboxLoop != null && !mailboxLoop.isDisposed()) mailboxLoop.dispose();
        mailbox.tryEmitComplete();
        scheduler.dispose();
    }
}