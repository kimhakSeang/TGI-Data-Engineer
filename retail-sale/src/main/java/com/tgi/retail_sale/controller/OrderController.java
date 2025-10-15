package com.tgi.retail_sale.controller;

import com.tgi.retail_sale.model.OrderRequest;
import com.tgi.retail_sale.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/order/v1")
public class OrderController {

    @Autowired
    private OrderService orderService;

    @PostMapping()
    public ResponseEntity<?> makeOrder(@RequestBody OrderRequest orderRequest){

        return ResponseEntity.ok(orderService.perform(orderRequest));
    }
}
