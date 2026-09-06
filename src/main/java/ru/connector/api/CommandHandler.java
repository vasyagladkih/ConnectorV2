package ru.connector.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.connector.api.docs.ApiStandardResponses;
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.service.ExchangeService;

@Tag(name = "Subscriptions", description = "Управление подписками на биржевые потоки рыночных данных")
@ApiStandardResponses
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class CommandHandler {

    private final ExchangeService exchangeService;

    @Operation(
            summary = "Создать подписку",
            description = "Регистрирует подписку на биржевой поток (trades/ticker/orderbook) и запускает WebSocket-стриминг"
    )
    @PostMapping(value = "/subscriptions", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<SubscriptionResponse> subscribe(@RequestBody @Valid Mono<SubscriptionDto> request) {
        return exchangeService.subscribe(request);
    }

    @Operation(
            summary = "Удалить подписку",
            description = "Отменяет подписку по уникальному ID и закрывает неиспользуемые WebSocket соединения"
    )
    @DeleteMapping("/subscriptions/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> unsubscribe(@PathVariable("id") Long id) {
        return exchangeService.unsubscribe(id);
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