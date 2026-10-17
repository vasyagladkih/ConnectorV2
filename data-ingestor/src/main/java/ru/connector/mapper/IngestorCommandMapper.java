package ru.connector.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.connector.models.Command;
import ru.connector.models.SubscriptionDto;
import ru.datacollector.common.model.BookTickerCommand;
import ru.datacollector.common.model.CreateSubscriptionRequest;
import ru.datacollector.common.model.OrderBookCommand;
import ru.datacollector.common.model.Symbol;
import ru.datacollector.common.model.TradesCommand;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface IngestorCommandMapper {

    default Symbol mapSymbol(String symbol) {
        return symbol != null ? Symbol.parse(symbol) : null;
    }

    default Command mapCommand(ru.datacollector.common.model.Command command) {
        if (command == null) return null;
        return switch (command) {
            case TradesCommand _ -> new Command.Trades();
            case BookTickerCommand _ -> new Command.BookTicker();
            case OrderBookCommand ob -> new Command.OrderBook(ob.depth());
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        };
    }

    default ru.datacollector.common.model.Command mapCommand(Command command) {
        if (command == null) return null;
        return switch (command) {
            case Command.Trades _ -> new TradesCommand("trades");
            case Command.BookTicker _ -> new BookTickerCommand("book_ticker");
            case Command.OrderBook ob -> new OrderBookCommand("orderbook", ob.depth());
        };
    }

    @Mapping(target = "symbol", expression = "java(mapSymbol(request.symbol()))")
    @Mapping(target = "command", expression = "java(mapCommand(request.command()))")
    SubscriptionDto toDto(CreateSubscriptionRequest request);

    @Mapping(target = "symbol", expression = "java(dto.symbol().toString())")
    @Mapping(target = "command", expression = "java(mapCommand(dto.command()))")
    @Mapping(target = "durationMinutes", ignore = true)
    CreateSubscriptionRequest toRequest(SubscriptionDto dto);
}
