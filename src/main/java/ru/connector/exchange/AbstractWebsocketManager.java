package ru.connector.exchange;

import org.jspecify.annotations.Nullable;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.exchange.registry.SubscriptionsRegistry;
import ru.connector.models.Action;

public abstract class AbstractWebsocketManager implements ExchangeManager {

    @Nullable
    protected final SubscriptionsRegistry registry;

    protected AbstractWebsocketManager(@Nullable SubscriptionsRegistry registry) {
        this.registry = registry;
    }

    public abstract String translate(SubscriptionDto request, Action action);
}