package ru.connector.api;

import jakarta.validation.Valid;
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
import ru.connector.api.dto.Request;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.service.ExchangeService;

@RestController
@RequestMapping("/api")
public class CommandHandler {

    private final ExchangeService exchangeService;

    public CommandHandler(ExchangeService exchangeService) {
        this.exchangeService = exchangeService;
    }

    @PostMapping(value = "/subscriptions", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<SubscriptionResponse> subscribe(@RequestBody @Valid Mono<Request> request) {
        return exchangeService.subscribe(request);
    }

    @DeleteMapping("/subscriptions/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> unsubscribe(@PathVariable Long id) {
        return exchangeService.unsubscribe(id);
    }

    @GetMapping("/subscriptions")
    public Flux<SubscriptionResponse> activeSubscriptions() {
        return exchangeService.activeSubscriptions();
    }
}