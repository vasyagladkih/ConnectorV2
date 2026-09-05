package ru.connector.exceptions;

import java.io.Serial;

public class NotFoundExchangeException extends ConnectorException {

    @Serial
    private static final long serialVersionUID = 1L;

    public NotFoundExchangeException(String message) {
        super(message);
    }
}
