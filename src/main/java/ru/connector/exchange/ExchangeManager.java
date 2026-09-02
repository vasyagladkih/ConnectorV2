package ru.connector.exchange;

import ru.connector.command.Request;

public interface ExchangeManager {
    void subscribe(Request request);
    void unsubscribe(Request request);
    void shutdown();
}