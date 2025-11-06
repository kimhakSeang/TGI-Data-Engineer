package com.tgi.retail_sale.config.bean;

import com.tgi.retail_sale.mapper.OrderMapper;
import com.tgi.retail_sale.model.OrderRequest;
import com.tgi.retail_sale.model.OrderResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@Configuration
public class OrderBeanConfig {
    @Autowired
    private OrderMapper orderMapper;

    @Bean("order-product")
    public Function<OrderRequest, OrderResponse> orderProductFunction(){
        return request-> {
            log.info("Start order product: {}", request);
            return orderMapper.toOrderResponse(request);
        };
    }

    @Bean
    public Consumer<OrderResponse> processOrder(){
        return orderResponse -> {
            System.out.println("Consume Request: "+ orderResponse);
        };
    }

    @Bean
    public Supplier<OrderRequest> orderSupplier() {
        return () -> {
            OrderRequest req = OrderRequest.builder()
                    .orderId(UUID.randomUUID().toString())
                    .build();
            System.out.println("☕ Producing: " + req);
            return req;
        };
    }

}
