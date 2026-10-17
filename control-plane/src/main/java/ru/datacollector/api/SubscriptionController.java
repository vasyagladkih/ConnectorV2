package ru.datacollector.api;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import ru.datacollector.common.api.SubscriptionsApi;
import ru.datacollector.common.model.CreateSubscriptionRequest;
import ru.datacollector.common.model.MarketType;
import ru.datacollector.common.model.SubscriptionResponse;
import ru.datacollector.common.model.SubscriptionStatus;
import ru.datacollector.service.SubscriptionService;

@RequiredArgsConstructor
@RestController
public class SubscriptionController implements SubscriptionsApi {

    private final SubscriptionService subscriptionService;

    @Override
    public ResponseEntity<SubscriptionResponse> createSubscription(CreateSubscriptionRequest body) {
        return null;
    }

    @Override
    public ResponseEntity<java.util.List<SubscriptionResponse>> getSubscriptions(SubscriptionStatus status, String exchange, MarketType market) {
        return null;
    }

    @Override
    public ResponseEntity<SubscriptionResponse> getSubscriptionById(Long id) {
        return null;
    }

    @Override
    public ResponseEntity<Void> cancelSubscription(Long id) {
        return null;
    }
}
