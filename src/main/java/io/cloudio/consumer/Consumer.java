package io.cloudio.consumer;

import java.time.Duration;
import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.task.Data;

public class Consumer extends BaseConsumer<String, Data> {

	private static Logger logger = LogManager.getLogger(Consumer.class);

	public Consumer(String groupId, Collection<String> topicNames) {
		super(groupId, topicNames);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected KafkaConsumer<String, Data> createConsumer() {
		return new KafkaConsumer<String, Data>(properties);
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	//CHANGED : Return type changed to ConsumerRecords so that task will commit if 
	//record handle is successful
	@Override
	public ConsumerRecords<String, Data> poll() throws Throwable {
		
		ConsumerRecords<String, Data> records = consumer.poll(Duration.ofSeconds(60));
		return records;

	}

	
	
	

	public void start() {
		// TODO Auto-generated method stub

	}

}
