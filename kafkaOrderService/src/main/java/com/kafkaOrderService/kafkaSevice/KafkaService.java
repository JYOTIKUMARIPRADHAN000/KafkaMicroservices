package com.kafkaOrderService.kafkaSevice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String _topic, String _key, String _event) {
		System.out.println("the event is" + _event + "from the topic " + _topic);
		kafkaTemplate.send(_topic, _key, _event);
	}

}