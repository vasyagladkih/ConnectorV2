package ru.connector.api.dto;

import ru.connector.models.Action;
import ru.connector.models.Symbol;

public record CommandResponse(
        String exchange,
        Action action,
        Symbol symbol
) {}