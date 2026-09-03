package ru.connector.exceptions;

public class SubscriptionNotFoundException extends ConnectorException {
    public SubscriptionNotFoundException(Long id) {
        super("Subscription not found: " + id);
    }
}
