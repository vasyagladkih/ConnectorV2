package ru.connector.exceptions;

import java.io.Serial;

public abstract class ConnectorException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ConnectorException(String message) {
        super(message);
    }

    public ConnectorException(String message, Throwable cause) {
        super(message, cause);
    }
}
