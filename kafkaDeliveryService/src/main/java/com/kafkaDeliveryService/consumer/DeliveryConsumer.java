package com.kafkaDeliveryService.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.kafkaDeliveryService.event.DeliveryEvent;
import com.kafkaDeliveryService.result.DeliveryResult;

import tools.jackson.databind.ObjectMapper;


@Service
public class DeliveryConsumer {

	private final ObjectMapper objectMapper;
	private final KafkaTemplate<String, String> kafkaTemplate;

	public DeliveryConsumer(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {

		this.objectMapper = objectMapper;
		this.kafkaTemplate = kafkaTemplate;
	}

	@KafkaListener(topics = "payment-success", groupId = "delivery-service")
	public void consumePaymentSuccess(String message) {

		try {

			DeliveryEvent event = objectMapper.readValue(message, DeliveryEvent.class);

			System.out.println("Received Payment Success Event: " + message);

			DeliveryResult result = new DeliveryResult();

			result.setEventId("DEL-" + event.getOrderId());
			result.setEventType("DELIVERY_CREATED");
			result.setOrderId(event.getOrderId());
			result.setCustomerId(event.getCustomerId());

			result.setTrackingNumber("TRK-" + event.getOrderId());

			result.setDeliveryAddress(event.getDeliveryAddress());

			result.setDeliveryStatus("CREATED");

			String json = objectMapper.writeValueAsString(result);

			kafkaTemplate.send("delivery-created", String.valueOf(event.getOrderId()), json);

			System.out.println("Delivery Created: " + json);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
