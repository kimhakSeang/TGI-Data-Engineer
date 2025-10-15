package com.tgi.retail_sale.kafka;

import com.tgi.retail_sale.model.OrderRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer extends DefaultProducer<OrderRequest>{

    @Value("${spring.cloud.stream.bindings.orderProduct-out-0.destination}")
    protected String topic;

    public OrderProducer(StreamBridge streamBridge) {
        super(streamBridge);
    }

    public boolean send(OrderRequest orderRequest, String key) {
        return sendMessage(topic, orderRequest, key);
    }
}
