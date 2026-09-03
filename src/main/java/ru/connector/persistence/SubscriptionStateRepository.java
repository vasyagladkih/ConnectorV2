package ru.connector.persistence;

import ru.connector.command.SubscriptionKey;

import java.util.Set;

public interface SubscriptionStateRepository {

    void saveSubscription(SubscriptionKey key);

    void removeSubscription(SubscriptionKey key);

    Set<SubscriptionKey> loadAllSubscriptions();
}
