package com.kafkaOrderService.repository;

import org.springframework.data.repository.CrudRepository;

import com.kafkaOrderService.entity.OrderEntity;



public interface OrderRepository extends CrudRepository<OrderEntity, Long> {

}

