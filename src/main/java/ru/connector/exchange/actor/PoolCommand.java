package ru.connector.exchange.actor;

import reactor.core.publisher.Sinks;
import ru.connector.api.dto.SubscriptionDto;

import ru.connector.models.StreamKey;

/**
 * Команды управления пулом соединений группы (GroupPoolActor).
 */
public sealed interface PoolCommand {

    /**
     * Команда на добавление подписки в пул.
     *
     * @param request параметры подписки
     * @param reply реактивное подтверждение завершения операции
     */
    record Subscribe(SubscriptionDto request, Sinks.One<Void> reply) implements PoolCommand {}

    /**
     * Команда на удаление подписки из пула.
     *
     * @param key ключ потока подписки
     * @param reply реактивное подтверждение завершения операции
     */
    record Unsubscribe(StreamKey key, Sinks.One<Void> reply) implements PoolCommand {}

    /**
     * Команда на закрытие всего пула и освобождение ресурсов.
     *
     * @param reply реактивное подтверждение завершения операции
     */
    record Close(Sinks.One<Void> reply) implements PoolCommand {}
}
