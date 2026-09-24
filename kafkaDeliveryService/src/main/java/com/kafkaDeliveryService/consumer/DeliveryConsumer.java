package com.kafkaDeliveryService.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
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

	@RetryableTopic(attempts = "3")
	@KafkaListener(topics = "payment-success", groupId = "delivery-service")
	public void consumePaymentSuccess(ConsumerRecord<String, String> record) {

	    String message = record.value();

	    System.out.println(
	        "Received → Topic: " + record.topic()
	        + " | Partition: " + record.partition()
	        + " | Offset: " + record.offset()
	        + " | Key: " + record.key()
	        + " | Message: " + message
	    );
		try {

			DeliveryEvent event = objectMapper.readValue(message, DeliveryEvent.class);

		    if (event.getOrderId() == 9999) {
		        throw new RuntimeException("Simulated delivery processing failure");
		    }

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
			 // IMPORTANT: let Spring Kafka know processing failed
		    throw new RuntimeException(e);
		}
	}
	
	@DltHandler
	public void handleDlt(ConsumerRecord<String, String> record) {

	    System.out.println(
	        "PAYMENT SUCCESS DLT MESSAGE RECEIVED → "
	        + "Topic: " + record.topic()
	        + " | Partition: " + record.partition()
	        + " | Offset: " + record.offset()
	        + " | Key: " + record.key()
	        + " | Message: " + record.value()
	    );
	}
}
