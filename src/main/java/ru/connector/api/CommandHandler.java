package ru.connector.api;

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
import ru.connector.api.dto.SubscriptionDto;
import ru.connector.api.dto.SubscriptionResponse;
import ru.connector.service.ExchangeService;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class CommandHandler {

    private final ExchangeService exchangeService;

    @PostMapping(value = "/subscriptions", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<SubscriptionResponse> subscribe(@RequestBody @Valid Mono<SubscriptionDto> request) {
        return exchangeService.saveToJournal(request);
    }

    @DeleteMapping("/subscriptions/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> unsubscribe(@PathVariable("id") String id) {
        return exchangeService.saveToJournalTombstone(id);
    }

    @GetMapping(value = "/subscriptions", produces = MediaType.APPLICATION_JSON_VALUE)
    public Flux<SubscriptionResponse> activeSubscriptions() {
        return exchangeService.activeSubscriptions();
    }
}