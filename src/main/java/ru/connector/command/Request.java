package ru.connector.command;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record Request(
        @NotBlank(message = "Exchange must not be blank")
        String exchange,

        @NotNull(message = "Action is required")
        Action action,

        @NotNull(message = "Market type is required")
        MarketType market,

        @NotNull(message = "Symbol is required")
        @Valid
        Symbol symbol,

        @NotNull(message = "Command is required")
        @Valid
        Command command
) {}