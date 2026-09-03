package ru.connector.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import ru.connector.exchange.ExchangeManager;
import ru.connector.service.ExchangeService;

import java.util.Map;

import static org.mockito.Mockito.*;

class CommandHandlerTest {

    private WebTestClient testClient;
    private ExchangeManager mockKucoinManager;

    @BeforeEach
    void setUp() {
        mockKucoinManager = mock(ExchangeManager.class);
        ExchangeService exchangeService = new ExchangeService(Map.of("KUCOIN", mockKucoinManager));
        CommandHandler commandHandler = new CommandHandler(exchangeService);

        testClient = WebTestClient.bindToController(commandHandler)
                .controllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void testSubscribeWithIdReturnsAcceptedAndId() {
        String jsonPayload = """
                {
                    "exchange": "KUCOIN",
                    "market": "SPOT",
                    "symbol": "BTC-USDT",
                    "command": {
                        "type": "trades"
                    }
                }
                """;

        testClient.post()
                .uri("/api/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonPayload)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.id").isNotEmpty()
                .jsonPath("$.exchange").isEqualTo("KUCOIN")
                .jsonPath("$.market").isEqualTo("SPOT")
                .jsonPath("$.symbol").isEqualTo("BTC-USDT")
                .jsonPath("$.status").doesNotExist();

        verify(mockKucoinManager, times(1)).subscribe(any());
    }

    @Test
    void testUnsubscribeByIdReturnsNoContent() {
        String jsonPayload = """
                {
                    "exchange": "KUCOIN",
                    "market": "SPOT",
                    "symbol": "ETH-USDT",
                    "command": {
                        "type": "trades"
                    }
                }
                """;

        Long subId = testClient.post()
                .uri("/api/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonPayload)
                .exchange()
                .expectStatus().isAccepted()
                .returnResult(Map.class)
                .getResponseBody()
                .map(m -> ((Number) m.get("id")).longValue())
                .blockFirst();

        testClient.delete()
                .uri("/api/subscriptions/" + subId)
                .exchange()
                .expectStatus().isNoContent();

        verify(mockKucoinManager, times(1)).unsubscribe(any());
    }

    @Test
    void testUnsubscribeNonExistentIdReturnsNotFound() {
        testClient.delete()
                .uri("/api/subscriptions/999999")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.message").isEqualTo("Subscription not found: 999999");
    }

    @Test
    void testActiveSubscriptionsReturnsList() {
        String jsonPayload = """
                {
                    "exchange": "KUCOIN",
                    "market": "SPOT",
                    "symbol": "SOL-USDT",
                    "command": {
                        "type": "trades"
                    }
                }
                """;

        testClient.post()
                .uri("/api/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonPayload)
                .exchange()
                .expectStatus().isAccepted();

        testClient.get()
                .uri("/api/subscriptions")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$").isArray()
                .jsonPath("$[0].symbol").isEqualTo("SOL-USDT");
    }

    @Test
    void testUnsupportedExchangeReturnsNotFound() {
        String jsonPayload = """
                {
                    "exchange": "OKX",
                    "market": "SPOT",
                    "symbol": "BTC-USDT",
                    "command": {
                        "type": "trades"
                    }
                }
                """;

        testClient.post()
                .uri("/api/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonPayload)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.message").isEqualTo("Exchange not supported: OKX");
    }

    @Test
    void testMissingRequiredFieldsReturnsBadRequestWithValidationErrors() {
        String invalidPayload = """
                {
                    "exchange": ""
                }
                """;

        testClient.post()
                .uri("/api/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(invalidPayload)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.status").isEqualTo(400)
                .jsonPath("$.error").isEqualTo("Validation Failed")
                .jsonPath("$.details").isArray();
    }

    @Test
    void testInvalidSymbolFormatReturnsBadRequest() {
        String invalidSymbolPayload = """
                {
                    "exchange": "KUCOIN",
                    "market": "SPOT",
                    "symbol": "INVALID",
                    "command": {
                        "type": "trades"
                    }
                }
                """;

        testClient.post()
                .uri("/api/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(invalidSymbolPayload)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.status").isEqualTo(400);
    }
}
