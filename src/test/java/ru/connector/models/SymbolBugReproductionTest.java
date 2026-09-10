package ru.connector.models;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolBugReproductionTest {

    @Test
    @DisplayName("Symbol.parse() обязан сохранять дату экспирации для фьючерсов вида BTC-USDT-240329")
    void shouldPreserveExpiryDateWhenParsingFuturesSymbol() {
        String inputFuturesSymbol = "BTC-USDT-240329";

        Symbol symbol = Symbol.parse(inputFuturesSymbol);

        assertTrue(symbol.toString().contains("240329"),
                "Дата экспирации 240329 должна быть сохранена в Symbol");
    }
}
