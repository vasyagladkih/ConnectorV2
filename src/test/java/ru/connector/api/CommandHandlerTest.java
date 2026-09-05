package ru.connector.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.connector.exceptions.ExchangeConnectionException;
import ru.connector.exchange.impl.kucoin.KucoinManager;
import ru.connector.exchange.registry.SubscriptionsRegistry;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.service.ExchangeService;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommandHandlerTest {

    private WebTestClient client;

    @Mock
    private WebSocketClient mockWsClient;

    @Mock
    private KafkaSubscriptionPublisher mockKafkaPublisher;

    @Mock
    private KafkaRawDataPublisher mockRawPublisher;

    private List<String> sentWsFrames;

    @BeforeEach
    void setUp() {
        sentWsFrames = new CopyOnWriteArrayList<>();

        // Симуляция сетевого WebSocket подключения
        lenient().when(mockWsClient.execute(any(URI.class), any(WebSocketHandler.class))).thenAnswer(invocation -> {
            WebSocketHandler handler = invocation.getArgument(1);
            WebSocketSession session = mock(WebSocketSession.class);

            when(session.receive()).thenReturn(Flux.never());
            when(session.send(any())).thenAnswer(sendInv -> {
                Publisher<WebSocketMessage> publisher = sendInv.getArgument(0);
                return Flux.from(publisher)
                        .doOnNext(msg -> sentWsFrames.add(msg.getPayloadAsText()))
                        .then();
            });
            when(session.textMessage(anyString())).thenAnswer(textInv -> {
                WebSocketMessage msg = mock(WebSocketMessage.class);
                when(msg.getPayloadAsText()).thenReturn(textInv.getArgument(0));
                return msg;
            });

            return handler.handle(session);
        });

        lenient().when(mockKafkaPublisher.publish(any(), any())).thenReturn(Mono.empty());
        lenient().when(mockKafkaPublisher.publishTombstone(any())).thenReturn(Mono.empty());

        // Сборка реального стека приложения
        SubscriptionsRegistry registry = new SubscriptionsRegistry();
        ObjectMapper objectMapper = new ObjectMapper();
        KucoinManager kucoinManager = new KucoinManager(registry, mockWsClient, mockRawPublisher, objectMapper);
        ExchangeService exchangeService = new ExchangeService(mockKafkaPublisher, Map.of("KUCOIN", kucoinManager));

        client = WebTestClient.bindToController(new CommandHandler(exchangeService))
                .controllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    /**
     * Что проверяем: Успешное создание подписки на сделки BTC-USDT.
     * Что ждем:
     *   - HTTP 202 Accepted и JSON с ID=1;
     *   - Отправку события в Kafka (publish);
     *   - Отправку кадра subscribe в сокет биржи;
     *   - Появление записи в GET /api/subscriptions.
     */
    @Test
    @DisplayName("POST /api/subscriptions: Успешная регистрация и отправка фрейма в WS")
    void shouldSubscribeSuccessfully() {
        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.id").isEqualTo(1)
                .jsonPath("$.exchange").isEqualTo("KUCOIN")
                .jsonPath("$.market").isEqualTo("SPOT")
                .jsonPath("$.symbol").isEqualTo("BTC-USDT");

        verify(mockKafkaPublisher, times(1)).publish(eq(1L), any());
        assertWsFrameSent("subscribe");

        getActiveSubscriptions()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(1)
                .jsonPath("$[0].id").isEqualTo(1);
    }

    /**
     * Что проверяем: Повторный запрос той же подписки (идемпотентность).
     * Что ждем:
     *   - HTTP 202 с тем же ID=1;
     *   - БЕЗ повторной отправки фрейма в сокет и БЕЗ дублирования в Kafka.
     */
    @Test
    @DisplayName("POST /api/subscriptions: Идемпотентность — повторный запрос возвращает тот же ID")
    void shouldBeIdempotentOnDuplicateSubscription() {
        // Первый вызов
        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted()
                .expectBody().jsonPath("$.id").isEqualTo(1);

        // Повторный вызов
        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted()
                .expectBody().jsonPath("$.id").isEqualTo(1);

        verify(mockKafkaPublisher, times(1)).publish(eq(1L), any());
        assertEquals(1, sentWsFrames.size(), "WebSocket frame must not be duplicated");
    }

    /**
     * Что проверяем: Отписку от существующего потока по ID.
     * Что ждем:
     *   - HTTP 204 No Content;
     *   - Отправку кадра unsubscribe в биржевой сокет;
     *   - Запись tombstone в Kafka;
     *   - Удаление из списка активных подписок.
     */
    @Test
    @DisplayName("DELETE /api/subscriptions/{id}: Успешная отписка и очистка состояния")
    void shouldUnsubscribeSuccessfully() {
        // Создаем подписку
        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted();

        // Отписываемся
        deleteSubscription(1L)
                .expectStatus().isNoContent();

        verify(mockKafkaPublisher, times(1)).publishTombstone(eq(1L));
        assertWsFrameSent("unsubscribe");

        getActiveSubscriptions()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(0);
    }

    /**
     * Что проверяем: Попытку отписки от несуществующего идентификатора.
     * Что ждем: HTTP 404 Not Found с сообщением об ошибке.
     */
    @Test
    @DisplayName("DELETE /api/subscriptions/{id}: 404 Not Found для неизвестного ID")
    void shouldReturnNotFoundOnUnknownSubscriptionId() {
        deleteSubscription(999L)
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.message").isEqualTo("Subscription not found: 999");
    }

    /**
     * Что проверяем: Запрос подписки на неподдерживаемую биржу (BINANCE).
     * Что ждем: HTTP 404 Not Found ("Exchange not supported: BINANCE").
     */
    @Test
    @DisplayName("POST /api/subscriptions: 404 Not Found для неподдерживаемой биржи")
    void shouldReturnNotFoundForUnsupportedExchange() {
        postFixture("fixtures/requests/subscribe-unknown-exchange.json")
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.message").isEqualTo("Exchange not supported: BINANCE");
    }

    /**
     * Что проверяем: Валидацию некорректных входных DTO.
     * Что ждем: HTTP 400 Bad Request для каждого случая нарушения контракта.
     */
    @Test
    @DisplayName("POST /api/subscriptions: 400 Bad Request при невалидном теле запроса")
    void shouldReturnBadRequestOnValidationErrors() {
        // 1. Пустое имя биржи
        postFixture("fixtures/requests/invalid-blank-exchange.json")
                .expectStatus().isBadRequest();

        // 2. Некорректный формат символа
        postFixture("fixtures/requests/invalid-symbol.json")
                .expectStatus().isBadRequest();

        // 3. Отсутствие обязательных полей
        postFixture("fixtures/requests/invalid-missing-fields.json")
                .expectStatus().isBadRequest();

        // 4. Синтаксически битый JSON
        postFixture("fixtures/requests/invalid-malformed.json")
                .expectStatus().isBadRequest();
    }

    /**
     * Что проверяем: Сценарий сбоя сети при подключении к WebSocket биржи.
     * Что ждем:
     *   - HTTP 502 Bad Gateway клиенту;
     *   - Автоматический откат (отправку tombstone в Kafka);
     *   - Отсутствие подписки в реестре.
     */
    @Test
    @DisplayName("POST /api/subscriptions: 502 Bad Gateway и откат транзакции при сбое сокета")
    void shouldRollbackStateWhenWebSocketConnectionFails() {
        // Имитируем падение сокета
        reset(mockWsClient);
        when(mockWsClient.execute(any(URI.class), any(WebSocketHandler.class)))
                .thenReturn(Mono.error(new ExchangeConnectionException("WebSocket connection refused")));

        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isEqualTo(502)
                .expectBody()
                .jsonPath("$.status").isEqualTo(502)
                .jsonPath("$.error").isEqualTo("Bad Gateway");

        verify(mockKafkaPublisher, times(1)).publishTombstone(eq(1L));

        getActiveSubscriptions()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(0);
    }

    // =========================================================================
    // Вспомогательные лаконичные методы (DSL для тестов)
    // =========================================================================

    private WebTestClient.ResponseSpec postFixture(String fixturePath) {
        return client.post()
                .uri("/api/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromResource(new ClassPathResource(fixturePath)))
                .exchange();
    }

    private WebTestClient.ResponseSpec deleteSubscription(long id) {
        return client.delete()
                .uri("/api/subscriptions/" + id)
                .exchange();
    }

    private WebTestClient.ResponseSpec getActiveSubscriptions() {
        return client.get()
                .uri("/api/subscriptions")
                .exchange();
    }

    private void assertWsFrameSent(String expectedAction) {
        boolean found = sentWsFrames.stream()
                .anyMatch(f -> f.contains("\"action\":\"" + expectedAction + "\"")
                               && f.contains("\"symbol\":\"" + "BTC-USDT" + "\""));
        assertTrue(found, () -> "Expected frame with action=" + expectedAction + " and symbol=" + "BTC-USDT" + ", but was: " + sentWsFrames);
    }
}