package ru.connector.exchange.impl.kucoin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import ru.connector.exchange.network.ExchangeAdapter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Адаптер биржи KuCoin для обработки служебных сообщений (ping/pong, welcome, ack).
 */
@Component
public class KucoinAdapter implements ExchangeAdapter {

    private static final Logger log = LoggerFactory.getLogger(KucoinAdapter.class);
    private static final int PING_INTERVAL = 18;
    private static final int MAX_SUBSCRIPTIONS = 100;
    private static final int PREVIEW_MAX_BYTES = 128;

    @Override
    public Flux<WebSocketMessage> createPingStream(WebSocketSession session) {
        return Flux.interval(Duration.ofSeconds(PING_INTERVAL))
                .map(seq -> session.textMessage("{\"id\":\"" + seq + "\",\"type\":\"ping\"}"));
    }

    @Override
    public boolean isServiceMsg(DataBuffer buffer) {
        // Проверяем первые байты без сдвига указателя чтения (readPosition),
        // чтобы не испортить буфер для consumer
        int readable = buffer.readableByteCount();
        int checkLen = Math.min(readable, PREVIEW_MAX_BYTES);
        byte[] preview = new byte[checkLen];
        for (int i = 0; i < checkLen; i++) {
            preview[i] = buffer.getByte(buffer.readPosition() + i);
        }
        String text = new String(preview, StandardCharsets.UTF_8);
        return text.contains("\"type\":\"welcome\"")
                || text.contains("\"type\":\"pong\"")
                || text.contains("\"type\":\"ack\"")
                || text.contains("\"type\":\"error\"")
                || text.contains("\"welcome\"")
                || text.contains("\"pong\"");
    }

    @Override
    public void handleServiceMsg(DataBuffer buffer) {
        if (log.isDebugEnabled()) {
            byte[] bytes = new byte[buffer.readableByteCount()];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = buffer.getByte(buffer.readPosition() + i);
            }
            log.debug("[KUCOIN-SERVICE] {}", new String(bytes, StandardCharsets.UTF_8));
        }
    }

    @Override
    public boolean isRecoverable(Throwable error) {
        // Ошибки сети и таймауты считаем восстановимыми для авто-реконнекта
        return true;
    }

    @Override
    public int getMaxSubscriptionsPerConnection() {
        return MAX_SUBSCRIPTIONS;
    }
}
