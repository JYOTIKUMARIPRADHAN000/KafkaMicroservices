package com.kafkaPaymentService.consumer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.kafkaPaymentService.event.PaymentEvent;
import com.kafkaPaymentService.result.PaymentResult;

import tools.jackson.databind.ObjectMapper;

@Service
public class PaymentConsumer {

	private final ObjectMapper objectMapper;
	private final KafkaTemplate<String, String> kafkaTemplate;
	private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

	public PaymentConsumer(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
		this.objectMapper = objectMapper;
		this.kafkaTemplate = kafkaTemplate;
	}

	@KafkaListener(topics = "order-created", groupId = "payment-service")
	public void consumeOrderCreated(ConsumerRecord<String, String> record) {

		String message = record.value();

		System.out.println("Received → Topic: " + record.topic() + " | Partition: " + record.partition() + " | Offset: "
				+ record.offset() + " | Key: " + record.key() + " | Message: " + message);

		try {

			PaymentEvent event = objectMapper.readValue(message, PaymentEvent.class);

			// check with idempotency
			String eventId = event.getEventId();

			if (!processedEvents.add(eventId)) {
				System.out.println("DUPLICATE EVENT DETECTED - Skipping eventId: " + eventId);
				return;
			}

			System.out.println("Received Order Created Event: " + message);

			// Simulate payment
			if (event.getAmount() <= 50000) {

				PaymentResult result = new PaymentResult();

				result.setEventId("PAY-" + event.getOrderId());
				result.setEventType("PAYMENT_SUCCESS");
				result.setOrderId(event.getOrderId());
				result.setCustomerId(event.getCustomerId());
				result.setAmount(event.getAmount());
				result.setPaymentId("TXN-" + event.getOrderId());
				result.setPaymentStatus("SUCCESS");
				result.setDeliveryAddress(event.getDeliveryAddress());

				String json = objectMapper.writeValueAsString(result);

				kafkaTemplate.send("payment-success", json);

				System.out.println("Payment Successful: " + json);

			} else {

				PaymentResult result = new PaymentResult();

				result.setEventId("PAY-" + event.getOrderId());
				result.setEventType("PAYMENT_FAILED");
				result.setOrderId(event.getOrderId());
				result.setCustomerId(event.getCustomerId());
				result.setAmount(event.getAmount());
				result.setPaymentId("TXN-" + event.getOrderId());
				result.setPaymentStatus("FAILED");
				result.setReason("INSUFFICIENT_FUNDS");
				result.setDeliveryAddress(event.getDeliveryAddress());

				String json = objectMapper.writeValueAsString(result);

				kafkaTemplate.send("payment-failed", json);

				System.out.println("Payment Failed: " + json);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}