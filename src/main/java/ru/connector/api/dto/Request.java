package ru.connector.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;
import ru.connector.models.Type;

public record Request(
        @NotBlank(message = "Exchange must not be blank")
        String exchange,

        @NotNull(message = "Market type is required")
        MarketType market,

        @NotNull(message = "Symbol is required")
        @Valid
        Symbol symbol,

        @NotNull(message = "Command is required")
        @Valid
        Command command
) {
    public Type type() {
        return Type.type(command);
    }
}