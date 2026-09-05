package ru.connector.exceptions;

import java.io.Serial;

public class ConnectionCapacityException extends ConnectorException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ConnectionCapacityException(String message) {
        super(message);
    }
}
