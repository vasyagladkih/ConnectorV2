package ru.connector.exchange;

import ru.connector.api.dto.SubscriptionDto;
import ru.connector.exchange.registry.SubscriptionsRegistry;
import ru.connector.models.Action;

public abstract class AbstractWebsocketManager implements ExchangeManager {

    protected final SubscriptionsRegistry registry;

    protected AbstractWebsocketManager(SubscriptionsRegistry registry) {
        this.registry = registry;
    }

    public abstract String translate(SubscriptionDto request, Action action);
}