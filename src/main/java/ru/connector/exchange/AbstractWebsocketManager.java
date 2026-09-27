package ru.connector.exchange;

import ru.connector.api.dto.SubscriptionDto;
import ru.connector.models.Action;

public abstract class AbstractWebsocketManager implements ExchangeManager {

    protected AbstractWebsocketManager() {}

    public abstract String translate(SubscriptionDto request, Action action);
}