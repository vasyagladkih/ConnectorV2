# ConnectorV2

Шлюз потоковой передачи рыночных данных с криптобирж в Apache Kafka в реальном времени.  
Клиент регистрирует подписку через REST API → шлюз подключается к WebSocket биржи (KuCoin Spot/Futures) → мультиплексирует стримы → транслирует сырые байтовые фреймы в Kafka.

Шлюз работает в режиме stateless ingress: не парсит JSON на горячем пути, минимизирует аллокации в памяти и гарантирует строгую последовательность событий по каждой торговой паре.

## Стек

- Java 26 (Records, Sealed Types, ZGC)
- Spring Boot 4.1.1 (Spring WebFlux, Reactor Netty)
- Apache Kafka 3.9.1 (KRaft) + spring-kafka
- Docker & Docker Compose
- SpringDoc OpenAPI 3.1

## Архитектура

```
Client ──(REST API)──► CommandHandler ──► ExchangeService ──► KucoinManager
                             │                                     │
                             ▼                                     ▼
                    (state / tombstones)                  ExchangeConnection (WebSocket)
                             │                                     │
                             ▼                                     │ (raw bytes)
                   KafkaSubscriptionPublisher                      ▼
                             │                            KafkaRawDataPublisher
                             ▼                                     │
                    market.subscriptions                           ▼
                     (compacted topic)                      market.data.raw
```

## Kafka topics

| Topic | Policy | Key | Value | Назначение |
|---|---|---|---|---|
| `market.data.raw` | delete | `EXCHANGE:MARKET:SYMBOL` (String) | `byte[]` | Сырой поток котировок с биржи |
| `market.subscriptions` | compact | `id` (String) | JSON / Tombstone (`null`) | Реестр активных подписок для синхронизации и recovery |

Ключ `EXCHANGE:MARKET:SYMBOL` гарантирует FIFO-порядок событий по инструменту внутри одной партиции без гонок при обработке консьюмерами.

## REST API

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/api/subscriptions` | Зарегистрировать подписку (trades, ticker, orderbook) |
| `DELETE` | `/api/subscriptions/{id}` | Отменить подписку и закрыть сокет при освобождении |
| `GET` | `/api/subscriptions` | Список активных подписок шлюза |

Swagger UI: `http://localhost:5556/swagger-ui/index.html`

## Локальный запуск

Требования: Docker Desktop.

```powershell
docker compose up -d --build
```

Проверить контейнеры:

```powershell
docker compose ps
```

Остановить:

```powershell
docker compose down
```

## Сервисы compose

| Сервис | Порт | Назначение |
|---|---|---|
| `connector-kafka` | 9092 | Kafka broker (KRaft) |
| `connector-kafka-ui` | 8080 | Web UI для управления топиками |
| `connector-gateway` | 5556 | Шлюз коннектора |

## Сборка и тесты

```powershell
.\gradlew.bat test
.\gradlew.bat bootJar
```
