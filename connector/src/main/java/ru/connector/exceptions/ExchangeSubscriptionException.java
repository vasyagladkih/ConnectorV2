package ru.connector.exceptions;

import lombok.Getter;

import java.io.Serial;

@Getter
public class ExchangeSubscriptionException extends ConnectorException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final int code;

    public ExchangeSubscriptionException(int code, String message) {
        super("Exchange rejected subscription (code=" + code + "): " + message);
        this.code = code;
    }
}
