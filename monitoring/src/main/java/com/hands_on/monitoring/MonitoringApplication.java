package com.hands_on.monitoring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class MonitoringApplication {

	public static void main(String[] args) {
		SpringApplication.run(MonitoringApplication.class, args);
		System.out.println("j");

	}

	@Bean
	public MessageProducer messageProducer(){
		return new MessageProducer();
	}

	public static class MessageProducer{

		@Value(value = "${message.topic.name}")
		private String topicName;

		@Autowired
		private KafkaTemplate<String, String> kafkaTemplate;

		public void sendMessage(String message){
			CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
			future.whenComplete((r, e) -> {
				if(e == null){
					System.out.println("Message sent = [" + message +"] successfully with offset ["+r.getRecordMetadata().offset()+"]");
				}
				else{
					System.out.println("Unable to send message = ["+message+"] because : ["+e.getMessage()+"] ");
				}
			});
		}


	}

	public static class MessageListener{

		private CountDownLatch latch = new CountDownLatch(3);

		@KafkaListener(topics = "${nessage.topic.name}", groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
		public void listenGroupFoo(String message){
			System.out.println("Received Message in group 'foo' :  " + message);
			latch.countDown();
		}
	}
}
