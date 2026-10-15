package ru.connector.context;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.api.CommandHandler;
import ru.connector.exchange.ExchangeManager;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.kafka.SubscriptionConsumer;
import ru.connector.service.ExchangeService;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Тест проверки инициализации Spring IoC контейнера (ApplicationContext).
 * <p>
 * Проверяет целостность графа зависимостей и корректность конфигурации всех бинов без внешнего брокера Kafka.
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false"
})
class ApplicationContextTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private CommandHandler commandHandler;

    @Autowired
    private ExchangeService exchangeService;

    @Autowired
    private ExchangeManager kucoinManager;

    @Autowired
    private KafkaSubscriptionPublisher kafkaSubscriptionPublisher;

    @Autowired
    private KafkaRawDataPublisher kafkaRawDataPublisher;

    @Autowired
    private SubscriptionConsumer subscriptionConsumer;

    @Autowired
    private WebSocketClient webSocketClient;

    @Test
    @DisplayName("Spring IoC контейнер: контекст успешно загружается и все ключевые бины инициализированы")
    void contextLoadsAndBeansAreInitialized() {
        assertNotNull(applicationContext, "ApplicationContext обязан быть загружен");
        assertNotNull(commandHandler, "CommandHandler обязан быть зарегистрирован в контейнере");
        assertNotNull(exchangeService, "ExchangeService обязан быть зарегистрирован в контейнере");
        assertNotNull(kucoinManager, "KucoinManager обязан быть зарегистрирован как бин биржи");
        assertNotNull(kafkaSubscriptionPublisher, "KafkaSubscriptionPublisher обязан быть инициализирован");
        assertNotNull(kafkaRawDataPublisher, "KafkaRawDataPublisher обязан быть инициализирован");
        assertNotNull(subscriptionConsumer, "SubscriptionConsumer обязан быть зарегистрирован в контейнере");
        assertNotNull(webSocketClient, "WebSocketClient обязан быть настроен в контексте");
    }
}
