package ru.connector.exchange;

import ru.connector.models.SubscriptionDto;
import ru.datacollector.common.model.Action;

public abstract class AbstractWebsocketManager implements ExchangeManager {

    protected AbstractWebsocketManager() {}

    public abstract String translate(SubscriptionDto request, Action action);
}