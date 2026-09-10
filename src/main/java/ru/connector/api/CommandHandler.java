package ru.connector.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.service.ExchangeService;

@RestController
@RequestMapping("/api")
@Tag(name = "Subscriptions", description = "Управление подписками на биржевые потоки данных")
public class CommandHandler {

    private final ExchangeService exchangeService;

    public CommandHandler(ExchangeService exchangeService) {
        this.exchangeService = exchangeService;
    }

    @Operation(
            summary = "Создать подписку",
            description = "Регистрирует новую подписку на биржевые данные (сделки, стакан, тикер)",
            responses = {
                    @ApiResponse(
                            responseCode = "202",
                            description = "Подписка принята в обработку",
                            content = @Content(
                                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                                    schema = @Schema(implementation = SubscriptionResponse.class),
                                    examples = @ExampleObject(
                                            value = "{\"exchange\":\"KUCOIN\",\"market\":\"SPOT\",\"symbol\":\"BTC-USDT\",\"type\":\"TRADES\"}"
                                    )
                            )
                    )
            }
    )
    @PostMapping(value = "/subscriptions", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<SubscriptionResponse> subscribe(@RequestBody @Valid Mono<SubscriptionDto> request) {
        return exchangeService.subscribe(request);
    }

    @Operation(
            summary = "Удалить подписку",
            description = "Отменяет подписку на биржевой поток и закрывает неиспользуемые WebSocket соединения"
    )
    @DeleteMapping(value = "/subscriptions", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> unsubscribe(@RequestBody @Valid Mono<SubscriptionDto> request) {
        return request.flatMap(exchangeService::unsubscribe);
    }

    @Operation(
            summary = "Список активных подписок",
            description = "Возвращает потоковый список всех действующих подписок в шлюзе"
    )
    @GetMapping(value = "/subscriptions", produces = MediaType.APPLICATION_JSON_VALUE)
    public Flux<SubscriptionResponse> activeSubscriptions() {
        return exchangeService.activeSubscriptions();
    }
}