package ru.connector.exceptions;

import java.io.Serial;

public class ExchangeConnectionException extends ConnectorException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ExchangeConnectionException(String message) {
        super(message);
    }

    public ExchangeConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
