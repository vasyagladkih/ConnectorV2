package ru.connector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.connector.command.Action;
import ru.connector.command.Request;
import ru.connector.exception.NotFoundExchangeException;
import ru.connector.exchange.ExchangeManager;

import java.util.Map;

@Service
public class ExchangeService {

    private static final Logger log = LoggerFactory.getLogger(ExchangeService.class);
    private final Map<String, ExchangeManager> managers;

    public ExchangeService(Map<String, ExchangeManager> managers) {
        this.managers = managers;
    }

    public Mono<ResponseEntity<String>> execute(Mono<Request> requestMono) {
        return requestMono.flatMap(this::execute);
    }

    public Mono<ResponseEntity<String>> execute(Request req) {
        ExchangeManager manager = managers.get(req.exchange().toUpperCase());
        if (manager == null) {
            return Mono.error(new NotFoundExchangeException("Exchange not supported: " + req.exchange()));
        }

        if (req.action() == Action.SUBSCRIBE) {
            manager.subscribe(req);
        } else {
            manager.unsubscribe(req);
        }

        log.info("Executed command {} for {} on {}", req.action(), req.symbol(), req.exchange());

        String jsonResponse = String.format(
                "{\"status\":\"ACCEPTED\",\"action\":\"%s\",\"symbol\":\"%s\",\"exchange\":\"%s\"}",
                req.action(), req.symbol(), req.exchange()
        );

        return Mono.just(ResponseEntity.accepted()
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonResponse));
    }
}
