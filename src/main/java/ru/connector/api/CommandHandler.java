package ru.connector.api;

import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import ru.connector.command.Request;
import ru.connector.service.ExchangeService;

@RestController
@RequestMapping("/api")
public class CommandHandler {

    private static final Logger log = LoggerFactory.getLogger(CommandHandler.class);

    private final ExchangeService exchangeService;

    public CommandHandler(ExchangeService exchangeService) {
        this.exchangeService = exchangeService;
    }

    @PostMapping(value = "/command", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<String>> handleCommand(@RequestBody @Valid Request request) {
        log.debug("Received command request: {}", request);
        return exchangeService.execute(request);
    }
}