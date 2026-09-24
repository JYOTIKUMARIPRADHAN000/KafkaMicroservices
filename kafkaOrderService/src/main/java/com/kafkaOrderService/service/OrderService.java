package com.kafkaOrderService.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.kafkaOrderService.entity.OrderEntity;
import com.kafkaOrderService.event.OrderCreatedEvent;
import com.kafkaOrderService.kafkaSevice.KafkaService;
import com.kafkaOrderService.repository.OrderRepository;
import com.kafkaOrderService.request.OrderRequest;
import com.kafkaOrderService.response.OrderResponse;

import tools.jackson.databind.ObjectMapper;

@Service
public class OrderService {

	@Autowired
	OrderRepository orderRepository;

	@Autowired
	KafkaService kafkaService;

	public OrderResponse createOrder(OrderRequest orderRequest) {

		OrderEntity reqEntity = new OrderEntity();

		reqEntity.setCustomerId(orderRequest.getCustomerId());
		reqEntity.setCustomerName(orderRequest.getCustomerName());
		reqEntity.setProductId(orderRequest.getProductId());
		reqEntity.setProductName(orderRequest.getProductName());
		reqEntity.setQuantity(orderRequest.getQuantity());
		reqEntity.setAmount(orderRequest.getAmount());
		long reqProductId = orderRequest.getProductId();
		if (reqProductId != 0) {
			reqEntity.setStatus("placed");
		} else {
			reqEntity.setStatus("not placed");
		}

		reqEntity.setDeliveryAddress(orderRequest.getDeliveryAddress());

		OrderEntity orderResEntity = orderRepository.save(reqEntity);

		OrderCreatedEvent event = new OrderCreatedEvent();

		event.setEventId("EVT-" + orderResEntity.getOrderId());
		event.setEventType("ORDER_CREATED");
		event.setOrderId(orderResEntity.getOrderId());
		event.setCustomerId(orderResEntity.getCustomerId());
		event.setAmount(orderResEntity.getAmount());
		event.setDeliveryAddress(orderResEntity.getDeliveryAddress());

		String topic = "order-created";
		// Convert Event Object → JSON
		String eventJson = jsonToString(event);
		kafkaService.sendMessage(topic, String.valueOf(orderResEntity.getOrderId()), eventJson);

		OrderResponse response = new OrderResponse();

		response.setOrderId(orderResEntity.getOrderId());
		response.setStatus(orderResEntity.getStatus());

		return response;

	}

	private String jsonToString(OrderCreatedEvent orderCreatedEvent) {

		try {

			ObjectMapper objectMapper = new ObjectMapper();

			return objectMapper.writeValueAsString(orderCreatedEvent);

		} catch (Exception e) {

			throw new RuntimeException("Failed to convert event to JSON", e);
		}
	}

}
