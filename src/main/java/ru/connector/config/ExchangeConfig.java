package ru.connector.config;

import tools.jackson.databind.json.JsonMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.netty.http.client.HttpClient;

@Configuration
public class ExchangeConfig {

    @Bean
    public JsonMapper jsonMapper() {
        return JsonMapper.builder().build();
    }

    @Bean
    public WebSocketClient webSocketClient() {
        HttpClient httpClient = HttpClient.create()
                .compress(true);
        return new ReactorNettyWebSocketClient(httpClient);
    }
}