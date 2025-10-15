package com.tgi.retail_sale.service.impl;

import com.tgi.retail_sale.kafka.OrderProducer;
import com.tgi.retail_sale.model.OrderRequest;
import com.tgi.retail_sale.model.OrderResponse;
import com.tgi.retail_sale.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderProducer orderProducer;
    private final Random random = new Random();

    @Override
    public OrderResponse perform(OrderRequest orderRequest) {
        String orderId = "OP"+ (1000 + random.nextInt(9000));

        orderRequest.setOrderId(orderId);
        boolean sendStatus = orderProducer.send(orderRequest, orderId);

        return OrderResponse.builder()
                .orderId(orderId)
                .status(sendStatus ? "Order successfully": "Order Failed")
                .build();
    }
}
