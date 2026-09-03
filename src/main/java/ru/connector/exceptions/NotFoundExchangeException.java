package ru.connector.exceptions;

public class NotFoundExchangeException extends ConnectorException {
    public NotFoundExchangeException(String message) {
        super(message);
    }
}
