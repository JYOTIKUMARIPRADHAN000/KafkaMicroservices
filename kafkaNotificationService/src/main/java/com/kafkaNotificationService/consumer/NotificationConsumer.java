package com.kafkaNotificationService.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Service
public class NotificationConsumer {

	private final ObjectMapper objectMapper;

	public NotificationConsumer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@KafkaListener(topics = { "order-created", "payment-success", "payment-failed",
			"delivery-created" }, groupId = "notification-service")
	public void consumeNotificationEvent(String message) {

		JsonNode event = objectMapper.readTree(message);

		String eventType = event.get("eventType").asString();

		switch (eventType) {

		case "ORDER_CREATED":

			System.out.println("SMS SENT: Order " + event.get("orderId").asLong() + " has been created successfully.");

			break;

		case "PAYMENT_SUCCESS":

			System.out.println("SMS SENT: Your payment of Rs." + event.get("amount").asLong() + " for Order "
					+ event.get("orderId").asLong() + " was successful.");

			break;

		case "PAYMENT_FAILED":

			System.out.println("SMS SENT: Payment failed for Order " + event.get("orderId").asLong() + ". Reason: "
					+ event.get("reason").asString());

			break;

		case "DELIVERY_CREATED":

			System.out.println("SMS SENT: Delivery created for Order " + event.get("orderId").asLong()
					+ ". Tracking Number: " + event.get("trackingNumber").asString());

			break;

		default:

			System.out.println("Unknown event type: " + eventType);
		}
	}
}