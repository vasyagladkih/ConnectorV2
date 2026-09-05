package ru.connector.exceptions;

import java.io.Serial;

public class InvalidCommandException extends ConnectorException {

    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidCommandException(String message) {
        super(message);
    }

    public InvalidCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
