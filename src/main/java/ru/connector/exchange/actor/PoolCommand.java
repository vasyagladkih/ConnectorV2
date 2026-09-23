package ru.connector.exchange.actor;

import reactor.core.publisher.Sinks;
import ru.connector.api.dto.SubscriptionDto;

/**
 * Команды управления пулом соединений группы (GroupPoolActor).
 */
public sealed interface PoolCommand {

    /**
     * Команда на добавление подписки в пул.
     *
     * @param id идентификатор подписки
     * @param request параметры подписки
     * @param reply реактивное подтверждение завершения операции
     */
    record Subscribe(Long id, SubscriptionDto request, Sinks.One<Void> reply) implements PoolCommand {}

    /**
     * Команда на удаление подписки из пула.
     *
     * @param id идентификатор подписки
     * @param reply реактивное подтверждение завершения операции
     */
    record Unsubscribe(Long id, Sinks.One<Void> reply) implements PoolCommand {}

    /**
     * Команда на закрытие всего пула и освобождение ресурсов.
     *
     * @param reply реактивное подтверждение завершения операции
     */
    record Close(Sinks.One<Void> reply) implements PoolCommand {}
}
