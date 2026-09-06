package ru.connector.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Schema(description = "Стандартизированная структура ответа при ошибке")
public record ErrorResponse(
        @Schema(description = "Метка времени UTC в формате ISO-8601", example = "2026-09-06T12:00:00Z")
        Instant timestamp,

        @Schema(description = "Числовой код HTTP статуса", example = "400")
        int status,

        @Schema(description = "Краткое наименование HTTP ошибки", example = "Bad Request")
        String error,

        @Schema(description = "Понятное сообщение о причине ошибки", example = "Subscription with id 100 not found")
        String message,

        @Schema(description = "URI запроса, на котором произошла ошибка", example = "/api/subscriptions/100")
        String path,

        @Schema(description = "Список деталей валидации полей (при наличии)", example = "[\"symbol: Base asset cannot be blank\"]")
        List<String> details
) {
    public static ErrorResponse of(int status, String error, String message, String path) {
        return new ErrorResponse(Instant.now(), status, error, message, path, List.of());
    }

    public static ErrorResponse of(int status, String error, String message, String path, List<String> details) {
        return new ErrorResponse(Instant.now(), status, error, message, path, details);
    }
}