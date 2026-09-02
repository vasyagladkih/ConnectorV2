package ru.connector.command;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public record Symbol(String base, String quote) {

    @JsonCreator
    public static Symbol parse(String value) {
        if (value == null || value.isBlank()) {throw new IllegalArgumentException("Symbol cannot be null or empty");}

        String[] parts = value.split("[-_/]");
        if (parts.length < 2) throw new IllegalArgumentException("Invalid symbol format: " + value);

        return new Symbol(parts[0].toUpperCase(), parts[1].toUpperCase());
    }

    @Override
    @JsonValue
    public String toString() {
        return base + "-" + quote;
    }
}