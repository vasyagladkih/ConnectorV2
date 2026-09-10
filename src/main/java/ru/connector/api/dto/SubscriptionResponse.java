package ru.connector.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import ru.connector.models.Command;
import ru.connector.models.MarketType;
import ru.connector.models.Symbol;

@Schema(description = "Модель зарегистрированной подписки на поток рыночных данных")
public record SubscriptionResponse(

        @Schema(description = "Идентификатор биржи", example = "KUCOIN")
        String exchange,

        @Schema(description = "Тип рынка", example = "SPOT")
        MarketType market,

        @Schema(description = "Торговая пара инструментов", example = "BTC-USDT", type = "string")
        Symbol symbol,

        @Schema(description = "Команда и параметры подписки")
        Command command
) {

    public static SubscriptionResponse toResponse(SubscriptionDto request) {
        return new SubscriptionResponse(
                request.exchange(),
                request.market(),
                request.symbol(),
                request.command()
        );
    }
}