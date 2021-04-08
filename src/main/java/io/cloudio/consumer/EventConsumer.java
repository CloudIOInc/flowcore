package io.cloudio.consumer;

import java.time.Duration;
import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.cloudio.messages.Settings;
import io.cloudio.task.Data;
import io.cloudio.task.Event;

public class EventConsumer extends BaseConsumer<String, String> {

	private static Logger logger = LogManager.getLogger(Consumer.class);

	public EventConsumer(String groupId, Collection<String> topicNames) {
		super(groupId, topicNames);
		// TODO Auto-generated constructor stub
	}

	@Override
	public KafkaConsumer<String, String> createConsumer() {
		//TODO: Update properties obj with deserilizer for events
		consumer = new KafkaConsumer<String, String>(properties);
		return consumer;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	//CHANGED : Return type changed to ConsumerRecords so that task will commit if 
	//record handle is successful
	@Override
	public ConsumerRecords<String, String> poll() throws Throwable {
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));
		return records;

	}

	
	

	
}