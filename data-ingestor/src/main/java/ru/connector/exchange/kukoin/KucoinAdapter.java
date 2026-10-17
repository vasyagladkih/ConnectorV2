package ru.connector.exchange.kukoin;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import ru.connector.exchange.ExchangeAdapter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Адаптер биржи KuCoin для обработки служебных сообщений (ping/pong, welcome, ack).
 */
@Slf4j
public class KucoinAdapter implements ExchangeAdapter {

    private static final int PING_INTERVAL = 18;
    private static final int MAX_SUBSCRIPTIONS = 100;

    private static final byte[] PONG = "pong".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ACK = "ack".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] WELCOME = "welcome".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ERROR = "error".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RESULT = "\"result\"".getBytes(StandardCharsets.US_ASCII);

    @Override
    public Flux<WebSocketMessage> createPingStream(WebSocketSession session) {
        return Flux.interval(Duration.ofSeconds(PING_INTERVAL))
                .map(seq -> session.textMessage("{\"id\":\"" + seq + "\",\"type\":\"ping\"}"));
    }

    @Override
    public boolean isServiceMsg(DataBuffer buffer) {
        return matchBytes(buffer, PONG)
               || matchBytes(buffer, ACK)
               || matchBytes(buffer, WELCOME)
               || matchBytes(buffer, ERROR)
               || matchBytes(buffer, RESULT);
    }

    private boolean matchBytes(DataBuffer buffer, byte[] target) {
        int start = buffer.readPosition();
        int len = buffer.readableByteCount();
        int targetLen = target.length;

        if (len < targetLen) return false;

        int limit = start + len - targetLen;
        for (int i = start; i <= limit; i++) {
            if (buffer.getByte(i) == target[0]) {
                boolean match = true;
                for (int j = 1; j < targetLen; j++) {
                    if (buffer.getByte(i + j) != target[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) return true;
            }
        }
        return false;
    }

    @Override
    public void handleServiceMsg(DataBuffer buffer) {
        if (log.isDebugEnabled()) {
            byte[] bytes = new byte[buffer.readableByteCount()];
            for (int i = 0; i < bytes.length; i++) {bytes[i] = buffer.getByte(buffer.readPosition() + i);}
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
