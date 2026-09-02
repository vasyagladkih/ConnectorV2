package ru.connector.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.netty.http.client.HttpClient;
import ru.connector.exchange.ExchangeManager;
import ru.connector.exchange.impl.KucoinManager;
import ru.connector.transport.KafkaPublisher;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class ExchangeConfig {

    @Bean
    public WebSocketClient webSocketClient() {
        // Кастомный HttpClient с увеличенным лимитом размера кадра для рыночных данных.
        HttpClient httpClient = HttpClient.create()
                .compress(true);
        return new ReactorNettyWebSocketClient(httpClient);
    }

    @Bean
    public KucoinManager kucoinManager(WebSocketClient client, KafkaPublisher publisher) {
        return new KucoinManager(client, publisher);
    }

    /**
     * Маппинг «имя биржи -> менеджер», используемый контроллером для маршрутизации команд.
     */
    @Bean
    public Map<String, ExchangeManager> exchangeManagers(KucoinManager kucoinManager) {
        Map<String, ExchangeManager> managers = new LinkedHashMap<>();
        managers.put("KUCOIN", kucoinManager);
        return managers;
    }
}