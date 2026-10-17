package ru.connector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"ru.connector", "ru.datacollector"})
public class ConnectorApplication {

    static void main(String[] args) {
        SpringApplication.run(ConnectorApplication.class, args);
    }
}