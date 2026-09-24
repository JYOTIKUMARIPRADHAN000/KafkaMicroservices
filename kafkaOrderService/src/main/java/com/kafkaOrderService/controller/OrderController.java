package com.kafkaOrderService.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkaOrderService.event.OrderCreatedEvent;
import com.kafkaOrderService.kafkaSevice.KafkaService;
import com.kafkaOrderService.request.OrderRequest;
import com.kafkaOrderService.response.OrderResponse;
import com.kafkaOrderService.service.OrderService;

import tools.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("order")
public class OrderController {
	@Autowired
	OrderService orderService;

	@Autowired
	KafkaService kafkaService;

	@PostMapping("/create")
	public OrderResponse createOrder(@RequestBody OrderRequest orderRequest) {
		return orderService.createOrder(orderRequest);
	}

	// to check with duplicate event for using the idempotency
	@PostMapping("/test-duplicate-event")
	public String testDuplicateEvent(@RequestBody OrderCreatedEvent event) {

		String json = jsonToString(event);

		kafkaService.sendMessage("order-created", String.valueOf(event.getOrderId()), json);

		kafkaService.sendMessage("order-created", String.valueOf(event.getOrderId()), json);

		return "Same event sent twice to Kafka";
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
