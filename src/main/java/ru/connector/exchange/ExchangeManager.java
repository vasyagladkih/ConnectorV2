package ru.connector.exchange;

import ru.connector.api.dto.Request;

public interface ExchangeManager {
    void subscribe(Request request);
    void unsubscribe(Request request);
    void shutdown();
}