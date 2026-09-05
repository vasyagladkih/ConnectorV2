package ru.connector.exceptions;

import java.io.Serial;

public class SubscriptionNotFoundException extends ConnectorException {

    @Serial
    private static final long serialVersionUID = 1L;

    public SubscriptionNotFoundException(Long id) {
        super("Subscription not found: " + id);
    }
}
