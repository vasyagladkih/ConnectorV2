package ru.connector.exceptions;

public abstract class ConnectorException extends RuntimeException {
    public ConnectorException(String message) {
        super(message);
    }

    public ConnectorException(String message, Throwable cause) {
        super(message, cause);
    }
}
