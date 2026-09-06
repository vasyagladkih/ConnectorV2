package ru.connector.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;
import ru.connector.models.Type;

@Schema(description = "Модель запроса на регистрацию новой подписки на биржевой поток")
public record SubscriptionDto(
        @Schema(description = "Идентификатор криптобиржи (например, KUCOIN, BINANCE)", example = "KUCOIN")
        @NotBlank(message = "Exchange must not be blank")
        String exchange,

        @Schema(description = "Тип рынка (SPOT или FUTURES)", example = "SPOT")
        @NotNull(message = "Market type is required")
        MarketType market,

        @Schema(description = "Торговая пара инструментов", example = "BTC-USDT", type = "string")
        @NotNull(message = "Symbol is required")
        @Valid
        Symbol symbol,

        @Schema(description = "Команда и параметры подписки (полиморфный объект, discriminator = type)")
        @NotNull(message = "Command is required")
        @Valid
        Command command
) {
    public Type type() {
        return Type.type(command);
    }
}