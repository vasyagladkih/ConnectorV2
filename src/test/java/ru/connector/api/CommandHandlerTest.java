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
    void testValidCommandReturnsAccepted() {
        String jsonPayload = """
                {
                    "exchange": "KUCOIN",
                    "action": "SUBSCRIBE",
                    "market": "SPOT",
                    "symbol": "BTC-USDT",
                    "command": {
                        "type": "trades"
                    }
                }
                """;

        testClient.post()
                .uri("/api/command")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonPayload)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.status").isEqualTo("ACCEPTED")
                .jsonPath("$.action").isEqualTo("SUBSCRIBE")
                .jsonPath("$.symbol").isEqualTo("BTC-USDT")
                .jsonPath("$.exchange").isEqualTo("KUCOIN");

        verify(mockKucoinManager, times(1)).subscribe(any());
    }

    @Test
    void testUnsupportedExchangeReturnsNotFound() {
        String jsonPayload = """
                {
                    "exchange": "OKX",
                    "action": "SUBSCRIBE",
                    "market": "SPOT",
                    "symbol": "BTC-USDT",
                    "command": {
                        "type": "trades"
                    }
                }
                """;

        testClient.post()
                .uri("/api/command")
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
                .uri("/api/command")
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
                    "action": "SUBSCRIBE",
                    "market": "SPOT",
                    "symbol": "INVALID",
                    "command": {
                        "type": "trades"
                    }
                }
                """;

        testClient.post()
                .uri("/api/command")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(invalidSymbolPayload)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.status").isEqualTo(400);
    }
}
