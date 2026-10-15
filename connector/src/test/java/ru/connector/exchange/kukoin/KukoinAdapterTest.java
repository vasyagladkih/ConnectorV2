package ru.connector.exchange.kukoin;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import ru.connector.exchange.ExchangeAdapter;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KukoinAdapterTest {

    private final ExchangeAdapter adapter = new KucoinAdapter();
    private final DefaultDataBufferFactory bufferFactory = new DefaultDataBufferFactory();

    private DataBuffer toBuffer(String json) {
        return bufferFactory.wrap(json.getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest(name = "Служебный фрейм должен определяться как service: {0}")
    @ValueSource(strings = {
            // Реальное служебное сообщение подтверждения подписки из инцидента
            "{\"id\":\"3082004417\",\"result\":true}",
            // Подтверждение подписки из официальной документации KuCoin
            "{\"id\":\"1545910660739\",\"result\":\"true\"}",
            // Подтверждение отписки из официальной документации KuCoin
            "{\"id\":\"1545910840805\",\"result\":\"true\"}",
            // Pong heartbeat из официальной документации KuCoin
            "{\"id\":\"1545910590801\",\"type\":\"pong\",\"ts\":1764577015970496582}",
            // Welcome handshake фрейм
            "{\"sessionId\":\"7245afa1-a57c-4f90-b4bd-90126387214b\",\"message\":\"welcome\",\"pingInterval\":30000}",
            // Сообщение об ошибке
            "{\"id\":\"1545910660739\",\"type\":\"error\",\"code\":400,\"data\":\"symbol not found\"}"
    })
    @DisplayName("Служебные сообщения KuCoin обязаны определяться как isServiceMsg=true")
    void shouldIdentifyServiceMessages(String serviceJson) {
        DataBuffer buffer = toBuffer(serviceJson);
        assertTrue(adapter.isServiceMsg(buffer),
                "Фрейм должен быть распознан как служебный: " + serviceJson);
    }

    @ParameterizedTest(name = "Рыночные данные обязаны пропускаться (isServiceMsg=false): {0}")
    @ValueSource(strings = {
            // Futures Trade (реальный формат сделок KuCoin UTA)
            "{\"T\":\"trade.FUTURES\",\"P\":1791755017750514192,\"d\":{\"p\":\"2527.09\",\"q\":\"1\",\"s\":\"ETHUSDTM\",\"S\":\"buy\",\"E\":1835063009642,\"ti\":\"1835063009642\",\"rpi\":false,\"M\":1791755017749000000}}",
            // Spot Trade из документации
            "{\"T\":\"trade.BTC-USDT\",\"d\":{\"s\":\"BTC-USDT\",\"p\":\"64000.5\",\"q\":\"0.12\",\"S\":\"buy\",\"t\":1725450000000}}",
            // Ticker из документации
            "{\"T\":\"ticker.BTC-USDT\",\"d\":{\"bestBid\":\"64000\",\"bestAsk\":\"64001\",\"ts\":1725450000000}}",
            // OrderBook L2 Depth c полем bids (проверка отсутствия ложного срабатывания на подстроку 'id' внутри 'bids')
            "{\"T\":\"obu.50.BTC-USDT\",\"d\":{\"bids\":[[\"64000\",\"1.5\"]],\"asks\":[[\"64001\",\"2.0\"]],\"ts\":1725450000000}}",
            // Klines (свечи) из документации
            "{\"T\":\"kline.1min.BTC-USDT\",\"d\":{\"candles\":[1725450000000,\"64000\",\"64100\",\"63900\",\"64050\",\"12.5\"]}}"
    })
    @DisplayName("Рыночные данные KuCoin UTA обязаны пропускаться (isServiceMsg=false)")
    void shouldPassMarketDataMessages(String marketJson) {
        DataBuffer buffer = toBuffer(marketJson);
        assertFalse(adapter.isServiceMsg(buffer),
                "Рыночные данные НЕ должны помечаться как служебные: " + marketJson);
    }
}