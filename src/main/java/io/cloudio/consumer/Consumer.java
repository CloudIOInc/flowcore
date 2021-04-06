package io.cloudio.consumer;

import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.cloudio.task.Data;

public class Consumer extends BaseConsumer<String, Data> {

	public Consumer(String groupId, Collection<String> topicNames) {
		super(groupId, topicNames);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected KafkaConsumer<String, Data> createConsumer() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Data> poll() throws Throwable {
		// TODO Auto-generated method stub
		return null;
		
	}

	public void start() {
		// TODO Auto-generated method stub
		
	}

}
