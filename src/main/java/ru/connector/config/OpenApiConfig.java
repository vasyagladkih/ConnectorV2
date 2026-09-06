package ru.connector.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("ConnectorV2 Market Data Gateway API")
                        .version("1.0.0")
                        .description("Реактивный шлюз для управления подписками на биржевые WebSocket-потоки и стриминга в Apache Kafka")
                        .contact(new Contact()
                                .name("Connector Support")));
    }
}
