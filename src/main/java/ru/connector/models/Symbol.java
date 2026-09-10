package ru.connector.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import jakarta.validation.constraints.NotBlank;

import java.util.Arrays;

public record Symbol(
        @NotBlank(message = "Base asset cannot be blank") String base,
        @NotBlank(message = "Quote asset cannot be blank") String quote
) {

    @JsonCreator
    public static Symbol parse(String value) {
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("Symbol cannot be null or empty");

        String trimmed = value.trim();
        String[] parts = trimmed.split("[-_/]");
        if (parts.length == 2) {
            return new Symbol(parts[0].toUpperCase(), parts[1].toUpperCase());
        }
        if (parts.length > 2) {
            String rest = String.join("-", Arrays.copyOfRange(parts, 1, parts.length)).toUpperCase();
            return new Symbol(parts[0].toUpperCase(), rest);
        }

        String upper = trimmed.toUpperCase();
        if (upper.endsWith("USDTM") && upper.length() > 5)
            return new Symbol(upper.substring(0, upper.length() - 5), "USDTM");
        if (upper.endsWith("USDM") && upper.length() > 4)
            return new Symbol(upper.substring(0, upper.length() - 4), "USDM");
        if (upper.endsWith("USDT") && upper.length() > 4)
            return new Symbol(upper.substring(0, upper.length() - 4), "USDT");

        throw new IllegalArgumentException("Invalid symbol format: " + value + ". Expected format like BTC-USDT, BTC_USDT, or BTC/USDT");
    }

    @Override
    @JsonValue
    public String toString() {
        return base + "-" + quote;
    }
}
