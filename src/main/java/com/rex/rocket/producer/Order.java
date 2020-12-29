package com.rex.rocket.producer;

import lombok.Data;

@Data
public class Order {
    private Long id;
    private String orderNo;
    private Long buyerId;
    private Long sellerId;
    private Long totalPrice;
    private Integer status;
}
