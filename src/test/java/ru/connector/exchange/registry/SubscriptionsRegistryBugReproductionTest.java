package ru.connector.exchange.registry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import ru.connector.exchange.network.ExchangeConnection;
import ru.connector.models.GroupKey;
import ru.connector.models.MarketType;
import ru.connector.models.Type;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class SubscriptionsRegistryBugReproductionTest {

    @Mock
    private WebSocketClient mockClient;

    @Test
    @DisplayName("Число активных слотов сокета не должно превышать MAX_CAPACITY (100) при параллельных запросах")
    void shouldNeverExceedMaxCapacityUnderConcurrency() throws InterruptedException {
        SubscriptionsRegistry registry = new SubscriptionsRegistry();
        GroupKey groupKey = GroupKey.of(MarketType.SPOT, Type.TRADES);

        ExchangeConnection conn = new ExchangeConnection(URI.create("ws://localhost"), mockClient, _ -> {});
        for (int i = 0; i < 99; i++) {
            conn.acquireSlot();
        }

        registry.getOrCreateConnection(groupKey, () -> conn);

        CountDownLatch t1GotConnLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(2);

        AtomicReference<ExchangeConnection> ref1 = new AtomicReference<>();
        AtomicReference<ExchangeConnection> ref2 = new AtomicReference<>();

        Thread t1 = new Thread(() -> {
            try {
                ExchangeConnection c = registry.getOrCreateConnection(groupKey, () ->
                        new ExchangeConnection(URI.create("ws://new"), mockClient, _ -> {}));
                ref1.set(c);
                t1GotConnLatch.countDown();

                Thread.sleep(50);
                c.acquireSlot();
            } catch (InterruptedException ignored) {
            } finally {
                finishLatch.countDown();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                t1GotConnLatch.await();
                ExchangeConnection c = registry.getOrCreateConnection(groupKey, () ->
                        new ExchangeConnection(URI.create("ws://new"), mockClient, _ -> {}));
                ref2.set(c);
                c.acquireSlot();
            } catch (InterruptedException ignored) {
            } finally {
                finishLatch.countDown();
            }
        });

        t1.start();
        t2.start();
        finishLatch.await();

        assertTrue(conn.getActiveSlots() <= 100,
                "Сокет не должен содержать более 100 слотов");
    }
}
