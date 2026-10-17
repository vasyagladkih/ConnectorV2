package ru.datacollector.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.datacollector.common.model.CreateSubscriptionRequest;
import ru.datacollector.common.model.SubscriptionResponse;
import ru.datacollector.common.model.SubscriptionStatus;

import java.time.OffsetDateTime;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface SubscriptionResponseMapper {

    @Mapping(target = "id", source = "id")
    @Mapping(target = "status", source = "status")
    @Mapping(target = "createdAt", source = "createdAt")
    @Mapping(target = "expiresAt", source = "expiresAt")
    @Mapping(target = "exchange", source = "request.exchange")
    @Mapping(target = "market", source = "request.market")
    @Mapping(target = "symbol", source = "request.symbol")
    @Mapping(target = "command", source = "request.command")
    SubscriptionResponse toResponse(
            CreateSubscriptionRequest request,
            Long id,
            SubscriptionStatus status,
            OffsetDateTime createdAt,
            OffsetDateTime expiresAt
    );
}
