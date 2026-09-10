package ru.connector.exchange.network;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExchangeConnectionBugReproductionTest {

    @Mock
    private WebSocketClient mockClient;

    @Test
    @DisplayName("Периодическая отправка ping-фреймов (heartbeat)")
    @SuppressWarnings("unchecked")
    void shouldSendPingHeartbeatFrames() {
        ExchangeConnection conn = new ExchangeConnection(
                URI.create("wss://x-push-spot.kucoin.com"),
                mockClient,
                _ -> {},
                Duration.ofMillis(50)
        );

        WebSocketSession session = mock(WebSocketSession.class);
        when(session.receive()).thenReturn(Flux.never());
        when(session.textMessage(anyString())).thenAnswer(inv -> {
            WebSocketMessage msg = mock(WebSocketMessage.class);
            when(msg.getPayloadAsText()).thenReturn(inv.getArgument(0));
            return msg;
        });

        ArgumentCaptor<Publisher<WebSocketMessage>> captor = ArgumentCaptor.forClass(Publisher.class);
        when(session.send(captor.capture())).thenReturn(Mono.never());

        conn.handle(session).subscribe();

        StepVerifier.create(Flux.from(captor.getValue()).map(WebSocketMessage::getPayloadAsText).take(2))
                .expectNextMatches(msg -> msg.contains("\"op\":\"ping\""))
                .expectNextMatches(msg -> msg.contains("\"op\":\"ping\""))
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("Служебные фреймы (welcome, pong) должны отфильтровываться и не попадать в Kafka")
    void shouldFilterOutControlFramesFromRawKafkaPublisher() {
        List<String> receivedPayloads = new ArrayList<>();

        ExchangeConnection conn = new ExchangeConnection(
                URI.create("wss://x-push-spot.kucoin.com"),
                mockClient,
                bytes -> receivedPayloads.add(new String(bytes, StandardCharsets.UTF_8))
        );

        WebSocketSession session = mock(WebSocketSession.class);
        lenient().when(session.send(any())).thenReturn(Mono.empty());

        String welcomeFrame = "{\"message\":\"welcome\",\"pingInterval\":18000}";
        when(session.receive()).thenReturn(Flux.just(
                new WebSocketMessage(WebSocketMessage.Type.TEXT,
                        new DefaultDataBufferFactory().wrap(welcomeFrame.getBytes(StandardCharsets.UTF_8)))
        ));

        conn.handle(session).block();

        assertTrue(receivedPayloads.isEmpty(),
                "Служебный фрейм welcome не должен попадать в котировки");
    }

    @Test
    @DisplayName("Метод send() обязан возвращать ошибку при переполнении буфера (FAIL_OVERFLOW)")
    void shouldSignalErrorWhenBackpressureBufferOverflows() {
        ExchangeConnection conn = new ExchangeConnection(
                URI.create("wss://x-push-spot.kucoin.com"),
                mockClient,
                bytes -> {}
        );

        WebSocketSession session = mock(WebSocketSession.class);
        lenient().when(session.receive()).thenReturn(Flux.never());
        lenient().when(session.send(any())).thenReturn(Mono.never());

        when(mockClient.execute(any(), any())).thenAnswer(inv -> {
            WebSocketHandler handler = inv.getArgument(1);
            return handler.handle(session);
        });

        conn.start();

        for (int i = 0; i < 1024; i++) {
            conn.send("frame-" + i).block();
        }

        StepVerifier.create(conn.send("overflow-frame"))
                .expectError()
                .verify();
    }

    @Test
    @DisplayName("Исключение в onMessage не должно закрывать общее WebSocket-соединение")
    void shouldNotCloseConnectionWhenOnMessageThrowsException() {
        ExchangeConnection conn = new ExchangeConnection(
                URI.create("wss://x-push-spot.kucoin.com"),
                mockClient,
                _ -> {
                    throw new RuntimeException("Simulated parser/Kafka error");
                }
        );

        WebSocketSession session = mock(WebSocketSession.class);
        lenient().when(session.send(any())).thenReturn(Mono.never());
        lenient().when(session.close()).thenReturn(Mono.empty());

        String testData = "{\"data\":\"test\"}";
        when(session.receive()).thenReturn(Flux.just(
                new WebSocketMessage(WebSocketMessage.Type.TEXT,
                        new DefaultDataBufferFactory().wrap(testData.getBytes(StandardCharsets.UTF_8)))
        ).concatWith(Flux.never()));

        conn.handle(session).subscribe();

        assertFalse(conn.isClosed(),
                "Сокет не должен закрываться из-за исключения при обработке сообщения");
    }
}
