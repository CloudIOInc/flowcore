
package io.cloudio.task;

import java.util.List;

import io.cloudio.consumer.Consumer;
import io.cloudio.producer.Producer;

public abstract class Task<E extends Event, D extends Data, O extends Data> {
	E event;
	String taskCode;
	String eventTopic;
	Consumer dataConsumer;
	Consumer eventConsumer;
	Producer producer;

	Task(String taskCode) {
		this.taskCode = taskCode;
		this.eventTopic = taskCode + "_events";
	}

	void onStart(Event event) {

	}

	void onEnd(Event event) {

	}

	public abstract void handleData(List<Data> data) ;

	public void start() {
		subscribeEvent(eventTopic); // listen to workflow engine for instructions
	}

	void subscribeEvent(String eventTopic) {
		// TODO Auto-generated method stub

	}

	private void handleEvent(E event) { // new event from wf engine
		unsubscribeEvent();
		this.event = event;
		subscribeData(event.fromTopic);
	}

	

	protected void post(List<O> data) {
		try {
			//TODO: Check what if data size is large
			producer.beginTransaction();
			for(Data obj :  data) {
				producer.send(event.toTopic, obj);
			}
			producer.commitTransaction();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void unsubscribeData() {
		dataConsumer.close();
		eventConsumer.start();
	}

	private void subscribeData(String fromTopic2) {

		while (true) {
			try {
				List<Data> data = dataConsumer.poll();
				handleData(data);
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void unsubscribeEvent() {
		eventConsumer.close();
	}

}
