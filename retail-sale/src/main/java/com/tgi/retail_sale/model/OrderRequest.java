package com.tgi.retail_sale.model;

import lombok.Data;

import java.time.Instant;

@Data
public class OrderRequest {
    private String orderId;
    private Instant orderDate;
    private Customer customer;
    private Product product;
    private int quantity;

    @Data
    private static class Customer{
        private String name;
        private int age;
    }

    @Data
    private static class Product{
        private String name;
        private String category;
        private int price;
    }

}
