package com.tgi.retail_sale.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OrderResponse {
    String orderId;
    String status;
}
