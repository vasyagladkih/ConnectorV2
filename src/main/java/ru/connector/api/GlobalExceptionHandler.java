package ru.connector.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebInputException;
import ru.connector.api.dto.ErrorResponse;
import ru.connector.exceptions.ConnectionCapacityException;
import ru.connector.exceptions.ExchangeConnectionException;
import ru.connector.exceptions.InvalidCommandException;
import ru.connector.exceptions.NotFoundExchangeException;
import ru.connector.exceptions.SubscriptionNotFoundException;

import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(SubscriptionNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleSubscriptionNotFound(SubscriptionNotFoundException ex, ServerHttpRequest request) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.of(
                        HttpStatus.NOT_FOUND.value(),
                        HttpStatus.NOT_FOUND.getReasonPhrase(),
                        ex.getMessage(),
                        request.getPath().value()
                ));
    }

    @ExceptionHandler(NotFoundExchangeException.class)
    public ResponseEntity<ErrorResponse> handleNotFoundExchange(NotFoundExchangeException ex, ServerHttpRequest request) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.of(
                        HttpStatus.NOT_FOUND.value(),
                        HttpStatus.NOT_FOUND.getReasonPhrase(),
                        ex.getMessage(),
                        request.getPath().value()
                ));
    }

    @ExceptionHandler(InvalidCommandException.class)
    public ResponseEntity<ErrorResponse> handleInvalidCommand(InvalidCommandException ex, ServerHttpRequest request) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of(
                        HttpStatus.BAD_REQUEST.value(),
                        HttpStatus.BAD_REQUEST.getReasonPhrase(),
                        ex.getMessage(),
                        request.getPath().value()
                ));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex, ServerHttpRequest request) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of(
                        HttpStatus.BAD_REQUEST.value(),
                        HttpStatus.BAD_REQUEST.getReasonPhrase(),
                        ex.getMessage(),
                        request.getPath().value()
                ));
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(WebExchangeBindException ex, ServerHttpRequest request) {
        List<String> details = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .toList();

        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of(
                        HttpStatus.BAD_REQUEST.value(),
                        "Validation Failed",
                        "One or more validation errors occurred",
                        request.getPath().value(),
                        details
                ));
    }

    @ExceptionHandler(ServerWebInputException.class)
    public ResponseEntity<ErrorResponse> handleServerWebInput(ServerWebInputException ex, ServerHttpRequest request) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of(
                        HttpStatus.BAD_REQUEST.value(),
                        "Bad Request",
                        ex.getReason() != null ? ex.getReason() : "Failed to read request body",
                        request.getPath().value()
                ));
    }

    @ExceptionHandler(ConnectionCapacityException.class)
    public ResponseEntity<ErrorResponse> handleCapacityException(ConnectionCapacityException ex, ServerHttpRequest request) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ErrorResponse.of(
                        HttpStatus.SERVICE_UNAVAILABLE.value(),
                        HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                        ex.getMessage(),
                        request.getPath().value()
                ));
    }

    @ExceptionHandler(ExchangeConnectionException.class)
    public ResponseEntity<ErrorResponse> handleExchangeConnection(ExchangeConnectionException ex, ServerHttpRequest request) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(ErrorResponse.of(
                        HttpStatus.BAD_GATEWAY.value(),
                        HttpStatus.BAD_GATEWAY.getReasonPhrase(),
                        ex.getMessage(),
                        request.getPath().value()
                ));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex, ServerHttpRequest request) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.of(
                        HttpStatus.INTERNAL_SERVER_ERROR.value(),
                        HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                        "An unexpected internal error occurred",
                        request.getPath().value()
                ));
    }
}