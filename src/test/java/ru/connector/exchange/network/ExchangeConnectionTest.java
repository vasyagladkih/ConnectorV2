package ru.connector.exchange.network;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExchangeConnectionTest {

    @Mock
    private WebSocketClient mockClient;

    @Mock
    private WebSocketSession mockSession;

    // =========================================================================
    // Баг 1: Race condition на activeSlots
    // =========================================================================

    /**
     * Что воспроизводит: 20 параллельных потоков вызывают acquireSlot()/releaseSlot().
     * Финальное значение activeSlots != 0 из-за lost updates.
     * Что ожидаем: activeSlots == 0 после всех операций (с AtomicInteger будет проходить).
     */
    @Test
    @DisplayName("activeSlots: race condition при 20 параллельных acquire/release")
    void shouldNotLoseSlotUpdatesUnderConcurrency() throws InterruptedException {
        ExchangeConnection conn = new ExchangeConnection(URI.create("ws://localhost"), mockClient, bytes -> {});

        int threads = 20;
        int iterations = 10000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(iterations);

        for (int i = 0; i < iterations; i++) {
            executor.submit(() -> {
                try {
                    conn.acquireSlot();
                    Thread.yield();
                } finally {
                    conn.releaseSlot();
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        int finalSlots = conn.getActiveSlots();
        assertEquals(0, finalSlots,
                () -> "Race condition detected: activeSlots = " + finalSlots
                        + " (expected 0). Fix: use AtomicInteger for activeSlots.");
    }

    // =========================================================================
    // Баг 2: Двойной вызов start() при параллельном send()
    // =========================================================================

    /**
     * Что воспроизводит: параллельные вызовы send() запускают client.execute() несколько раз,
     * Что ожидаем: client.execute() вызывается ровно один раз на соединение.
     */
    @Test
    @DisplayName("start(): client.execute() должен вызываться ровно один раз при параллельных send()")
    void shouldNotStartWebSocketTwiceOnConcurrentSend() throws Exception {
        lenient().when(mockSession.receive()).thenReturn(Flux.never());
        lenient().when(mockSession.send(any())).thenReturn(Mono.empty());
        lenient().when(mockSession.textMessage(anyString())).thenReturn(mock(org.springframework.web.reactive.socket.WebSocketMessage.class));

        int parallelCalls = 20;
        ExecutorService executor = Executors.newFixedThreadPool(parallelCalls);

        try {
            for (int attempt = 0; attempt < 25; attempt++) {
                reset(mockClient);
                when(mockClient.execute(any(URI.class), any(WebSocketHandler.class)))
                        .thenAnswer(inv -> {
                            WebSocketHandler handler = inv.getArgument(1);
                            return handler.handle(mockSession);
                        });

                ExchangeConnection conn = new ExchangeConnection(URI.create("ws://localhost"), mockClient, _ -> {});

                CountDownLatch startGun = new CountDownLatch(1);
                CountDownLatch latch = new CountDownLatch(parallelCalls);

                for (int i = 0; i < parallelCalls; i++) {
                    executor.submit(() -> {
                        try {
                            startGun.await();
                            conn.send("test-frame").block();
                        } catch (Exception ignored) {
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                startGun.countDown();
                assertTrue(latch.await(3, TimeUnit.SECONDS));

                verify(mockClient, times(1)).execute(any(URI.class), any(WebSocketHandler.class));
            }
        } finally {
            executor.shutdown();
        }
    }

    // =========================================================================
    // Баг 3: Утечка WebSocketSession при завершении outbound раньше inbound
    // =========================================================================

    /**
     * Что воспроизводит: Mono.firstWithSignal(inbound, outbound) завершается при первом сигнале.
     * Если outbound завершается первым (после outgoing.tryEmitComplete()), inbound (Flux.never())
     * не отписывается — сессия не закрывается, ресурсы утекают.
     * Что ожидаем: session.close() вызывается при завершении outbound.
     */
    @Test
    @DisplayName("handle(): session.close() должен вызываться при завершении outbound до inbound")
    void shouldCloseSessionWhenOutboundCompletesBeforeInbound() {
        ExchangeConnection conn = new ExchangeConnection(URI.create("ws://localhost"), mockClient, bytes -> {});

        when(mockSession.receive()).thenReturn(Flux.never());
        when(mockSession.send(any())).thenReturn(Mono.empty());
        when(mockSession.close()).thenReturn(Mono.empty());

        StepVerifier.create(conn.handle(mockSession))
                .then(conn::close)
                .verifyComplete();

        verify(mockSession, atLeastOnce()).close();
    }
}