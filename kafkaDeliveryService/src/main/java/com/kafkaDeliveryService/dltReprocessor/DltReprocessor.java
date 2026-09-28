package com.kafkaDeliveryService.dltReprocessor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
//
//@Service
//public class DltReprocessor {
//
//    private final KafkaTemplate<String, String> kafkaTemplate;
//
//    public DltReprocessor(KafkaTemplate<String, String> kafkaTemplate) {
//        this.kafkaTemplate = kafkaTemplate;
//    }
//
//    @KafkaListener(
//        topics = "payment-success-dlt",
//        groupId = "dlt-reprocessor"
//    )
//    public void reprocess(ConsumerRecord<String, String> record) {
//
//        System.out.println(
//            "DLT REPROCESSOR RECEIVED → "
//            + "Topic: " + record.topic()
//            + " | Partition: " + record.partition()
//            + " | Offset: " + record.offset()
//            + " | Key: " + record.key()
//            + " | Message: " + record.value()
//        );
//
//        // For this experiment, reprocess only our test order
//        if ("32718".equals(record.key())) {
//
//            kafkaTemplate.send(
//                "payment-success",
//                record.key(),
//                record.value()
//            );
//
//            System.out.println(
//                "DLT MESSAGE REPROCESSED → SENT BACK TO payment-success"
//            );
//        }
//    }
//}

import com.kafkaDeliveryService.event.DeliveryEvent;

import tools.jackson.databind.ObjectMapper;

//@Service
public class DltReprocessor {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;

	public DltReprocessor(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	@KafkaListener(topics = "payment-success-dlt", groupId = "dlt-reprocessor")
	public void reprocess(ConsumerRecord<String, String> record) {

		try {
			DeliveryEvent event = objectMapper.readValue(record.value(), DeliveryEvent.class);

			if ("pune".equalsIgnoreCase(event.getDeliveryAddress())) {
				
				 //if (record.partition() == 0 && record.offset() == 20) {

				System.out.println("REPROCESSING DLT → OrderId: " + event.getOrderId());

				kafkaTemplate.send("payment-success", record.key(), record.value());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}