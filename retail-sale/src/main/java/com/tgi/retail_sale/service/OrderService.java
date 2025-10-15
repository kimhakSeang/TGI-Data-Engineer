package com.tgi.retail_sale.service;

import com.tgi.retail_sale.model.OrderRequest;
import com.tgi.retail_sale.model.OrderResponse;
import org.springframework.stereotype.Component;

@Component
public interface OrderService {
    OrderResponse perform (OrderRequest orderRequest);
}
