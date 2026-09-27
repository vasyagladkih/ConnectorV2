package ru.connector.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.jspecify.annotations.NonNull;
import ru.connector.api.dto.SubscriptionDto;

import java.util.Locale;

public record StreamKey(
        String exchange,
        MarketType market,
        Symbol symbol,
        Type type
) {
    public static StreamKey from(SubscriptionDto request) {
        return new StreamKey(
                request.exchange().toUpperCase(Locale.ROOT),
                request.market(),
                request.symbol(),
                request.type()
        );
    }

    @JsonCreator
    public static StreamKey parse(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("StreamKey cannot be null or empty");
        }
        String[] parts = key.split(":");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Invalid StreamKey format: " + key + ". Expected format EXCHANGE:MARKET:SYMBOL:TYPE");
        }
        return new StreamKey(
                parts[0].toUpperCase(Locale.ROOT),
                MarketType.valueOf(parts[1].toUpperCase(Locale.ROOT)),
                Symbol.parse(parts[2]),
                Type.valueOf(parts[3].toUpperCase(Locale.ROOT))
        );
    }

    @Override
    @JsonValue
    public @NonNull String toString() {
        return exchange + ":" + market + ":" + symbol + ":" + type;
    }
}