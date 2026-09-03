package ru.connector.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.support.WebExchangeBindException;
import ru.connector.command.Request;
import ru.connector.exception.ConnectionCapacityException;
import ru.connector.exception.ExchangeConnectionException;
import ru.connector.exception.InvalidCommandException;
import ru.connector.exception.NotFoundExchangeException;

import static org.junit.jupiter.api.Assertions.*;

class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler handler;
    private MockServerHttpRequest request;

    @BeforeEach
    void setUp() {
        handler = new GlobalExceptionHandler();
        request = MockServerHttpRequest.post("/api/command").build();
    }

    @Test
    void testHandleNotFoundExchange() {
        NotFoundExchangeException ex = new NotFoundExchangeException("Exchange not supported: BINANCE");
        ResponseEntity<ErrorResponse> response = handler.handleNotFoundExchange(ex, request);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(404, response.getBody().status());
        assertEquals("Exchange not supported: BINANCE", response.getBody().message());
        assertEquals("/api/command", response.getBody().path());
    }

    @Test
    void testHandleInvalidCommand() {
        InvalidCommandException ex = new InvalidCommandException("Invalid command parameter");
        ResponseEntity<ErrorResponse> response = handler.handleInvalidCommand(ex, request);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(400, response.getBody().status());
        assertEquals("Invalid command parameter", response.getBody().message());
    }

    @Test
    void testHandleCapacityException() {
        ConnectionCapacityException ex = new ConnectionCapacityException("Connection pool full");
        ResponseEntity<ErrorResponse> response = handler.handleCapacityException(ex, request);

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(503, response.getBody().status());
        assertEquals("Connection pool full", response.getBody().message());
    }

    @Test
    void testHandleExchangeConnectionException() {
        ExchangeConnectionException ex = new ExchangeConnectionException("Connection timeout to WS gateway");
        ResponseEntity<ErrorResponse> response = handler.handleExchangeConnection(ex, request);

        assertEquals(HttpStatus.BAD_GATEWAY, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(502, response.getBody().status());
        assertEquals("Connection timeout to WS gateway", response.getBody().message());
    }

    @Test
    void testHandleValidationErrors() throws NoSuchMethodException {
        BeanPropertyBindingResult bindingResult = new BeanPropertyBindingResult(new Object(), "request");
        bindingResult.addError(new FieldError("request", "exchange", "Exchange must not be blank"));
        bindingResult.addError(new FieldError("request", "symbol", "Symbol is required"));

        MethodParameter parameter = new MethodParameter(
                CommandHandler.class.getMethod("handleCommand", Request.class), 0);
        WebExchangeBindException ex = new WebExchangeBindException(parameter, bindingResult);

        ResponseEntity<ErrorResponse> response = handler.handleValidationErrors(ex, request);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(400, response.getBody().status());
        assertEquals(2, response.getBody().details().size());
        assertTrue(response.getBody().details().stream().anyMatch(d -> d.contains("exchange: Exchange must not be blank")));
        assertTrue(response.getBody().details().stream().anyMatch(d -> d.contains("symbol: Symbol is required")));
    }
}
