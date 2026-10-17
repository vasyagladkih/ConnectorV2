package ru.connector.exchange.actor;

import reactor.core.publisher.Sinks;
import ru.datacollector.common.model.StreamKey;

/**
 * Команды управления отдельным WebSocket-соединением (ConnectionActor).
 */
public sealed interface ConnectionCommand {

    /**
     * Запрос на подписку на WebSocket-канале.
     *
     * @param key ключ потока котировок
     * @param frame тело сообщения подписки
     * @param reply подтверждение успешной отправки в исходящий поток сокета
     */
    record Subscribe(StreamKey key, String frame, Sinks.One<Void> reply) implements ConnectionCommand {}

    /**
     * Запрос на отписку от WebSocket-канала.
     *
     * @param key ключ потока котировок для удаления из активного набора
     * @param unsubscribeFrame тело сообщения отписки для отправки в сокет
     * @param reply подтверждение успешной отправки в исходящий поток сокета
     */
    record Unsubscribe(StreamKey key, String unsubscribeFrame, Sinks.One<Void> reply) implements ConnectionCommand {}

    /**
     * Запрос на закрытие и завершение работы соединения.
     *
     * @param reply подтверждение завершения
     */
    record Close(Sinks.One<Void> reply) implements ConnectionCommand {}
}
