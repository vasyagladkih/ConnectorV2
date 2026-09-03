package ru.connector.exception;

public class ExchangeConnectionException extends ConnectorException {
    public ExchangeConnectionException(String message) {
        super(message);
    }

    public ExchangeConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
