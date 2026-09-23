package ru.connector.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import jakarta.validation.constraints.NotBlank;
import org.jspecify.annotations.NonNull;

import java.util.Locale;

public record Symbol(
        @NotBlank(message = "Base asset cannot be blank") String base,
        @NotBlank(message = "Quote asset cannot be blank") String quote
) {

    @JsonCreator
    public static Symbol parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Symbol cannot be null or empty");
        }

        String trimmed = value.trim();
        String[] parts = trimmed.split("[-_/]");
        if (parts.length >= 2) {
            if (parts[0].isBlank() || parts[1].isBlank()) {
                throw new IllegalArgumentException("Base and quote assets cannot be blank: " + value);
            }
            return new Symbol(parts[0].toUpperCase(Locale.ROOT), parts[1].toUpperCase(Locale.ROOT));
        }

        String upper = trimmed.toUpperCase(Locale.ROOT);
        int len = upper.length();
        if (upper.endsWith("USDTM") && len > 5) {
            return new Symbol(upper.substring(0, len - 5), "USDTM");
        }
        if (len > 4) {
            String base = upper.substring(0, len - 4);
            if (upper.endsWith("USDM")) {
                return new Symbol(base, "USDM");
            }
            if (upper.endsWith("USDT")) {
                return new Symbol(base, "USDT");
            }
        }

        throw new IllegalArgumentException("Invalid symbol format: " + value + ". Expected format like BTC-USDT, BTC_USDT, or BTC/USDT");
    }

    @Override
    @JsonValue
    public @NonNull String toString() {
        return base + "-" + quote;
    }
}