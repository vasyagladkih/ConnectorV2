package ru.connector.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.netty.http.client.HttpClient;

@Configuration
public class ExchangeConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public WebSocketClient webSocketClient() {
        HttpClient httpClient = HttpClient.create()
                .compress(true);
        return new ReactorNettyWebSocketClient(httpClient);
    }
}