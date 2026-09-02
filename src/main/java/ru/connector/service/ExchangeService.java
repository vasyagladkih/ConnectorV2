package ru.connector.service;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.connector.command.Action;
import ru.connector.command.Request;
import ru.connector.exchange.ExchangeManager;

import java.util.Map;

@Service
public class ExchangeService {

    private final Map<String, ExchangeManager> managers;

    public ExchangeService(Map<String, ExchangeManager> managers) {
        this.managers = managers;
    }

    public Mono<ResponseEntity<String>> execute(Mono<Request> requestMono) {
        return requestMono.flatMap(req ->

                Mono.justOrEmpty(managers.get(req.exchange().toUpperCase()))
                .map(manager -> {

                    if (req.action() == Action.SUBSCRIBE) {manager.subscribe(req);} else {manager.unsubscribe(req);}

                    return ResponseEntity.accepted()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body("Command " + req.action() + " queued for " + req.symbol() + " on " + req.exchange());
                })

                .switchIfEmpty(Mono.error(new NotFoundExchangeException("Exchange not supported: " + req.exchange())))
        );
    }
}
