package ru.connector.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import ru.connector.command.Action;
import ru.connector.command.Request;
import ru.connector.exchange.ExchangeManager;
import ru.connector.service.ExchangeService;

import java.util.Map;

/**
 * WebFlux-замена Vert.x CommandServer.
 * Принимает команды подписки/отписки и маршрутизирует их в менеджер нужной биржи.
 */
@RestController
@RequestMapping("/api")
public class CommandHandler {

    private final ExchangeService exchangeService;

    private static final Logger log = LoggerFactory.getLogger(CommandHandler.class);

    public CommandHandler(ExchangeService exchangeService) {
        this.exchangeService = exchangeService;
    }

    @PostMapping(value = "/command", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<String>> handleCommand(@RequestBody @Valid Mono<Request> request) {
        return exchangeService.execute(request);
    }
}