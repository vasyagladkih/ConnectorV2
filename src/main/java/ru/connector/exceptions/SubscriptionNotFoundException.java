package ru.connector.exceptions;

import java.io.Serial;

public class SubscriptionNotFoundException extends ConnectorException {

    @Serial
    private static final long serialVersionUID = 1L;

    public SubscriptionNotFoundException(Object key) {
        super("Subscription not found: " + key);
    }
}
