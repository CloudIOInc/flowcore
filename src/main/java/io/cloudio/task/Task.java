
package io.cloudio.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import io.cloudio.consumer.Consumer;
import io.cloudio.consumer.EventConsumer;
import io.cloudio.messages.Settings;
import io.cloudio.producer.Producer;

public abstract class Task<E extends Event, D extends Data, O extends Data> {
	protected E event;
	private String taskCode;
	private String eventTopic;
	private Consumer dataConsumer;
	private EventConsumer eventConsumer;
	private Producer producer;
	private String groupId;

	private static Logger logger = LogManager.getLogger(Consumer.class);

	Task(String taskCode) {
		this.taskCode = taskCode;
		this.eventTopic = taskCode + "-events";
		this.groupId = taskCode + "-test1";
	}

	void onStart(Event event) {

	}

	void onEnd(Event event) {

	}

	public abstract void handleData(List<Data> data);

	public void start() {
		eventConsumer = new EventConsumer(groupId, Collections.singleton(eventTopic));
		eventConsumer.createConsumer(); //TODO : Revisit
		eventConsumer.subscribe();//TODO: Revisit
		subscribeEvent(eventTopic); // listen to workflow engine for instructions
	}

	void subscribeEvent(String eventTopic) {
		Throwable ex = null;
		// TODO: revisit this
		while (true) {
			try {
				ConsumerRecords<String, String> events = eventConsumer.poll();
				// TODO : Can below code go in consumer ??
				if (events != null && events.count() > 0) {
					for (TopicPartition partition : events.partitions()) {
						List<ConsumerRecord<String, String>> partitionRecords = events.records(partition);
						if (partitionRecords.size() == 0) {
							continue;
						}
						if (logger.isInfoEnabled()) {
							logger.info("Got {} events between {} & {} in {}", partitionRecords.size(),
									partitionRecords.get(0).offset(),
									partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
						}

						List<Event<Settings>> list = new ArrayList<>(partitionRecords.size());
						for (ConsumerRecord<String, String> record : partitionRecords) {
							String eventSting = record.value();
							if (eventSting == null) {
								continue;
							}
							Gson gson = new Gson(); //TODO: hanlde this in VALUE_DESERIALIZER_CLASS_CONFIG of consumer??
							Event<Settings> event = gson.fromJson(eventSting, Event.class); //TODO : FIXME : convert JSON string to Event object
							
							// TODO : Expect only one event ?
							handleEvent(event);
						}

						try {

							if (partitionRecords.size() > 0) {
								long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
								eventConsumer.commitSync(
										Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
							}
						} catch (WakeupException | InterruptException e) {
							throw e;
						} catch (Throwable e) {
							logger.catching(e);
							if (ex == null) {
								ex = e;
							}
							try {
								logger.error("Seeking back {} events between {} & {} in {}", partitionRecords.size(),
										partitionRecords.get(0).offset(),
										partitionRecords.get(partitionRecords.size() - 1).offset(),
										partition.toString());

								eventConsumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
							} catch (Throwable e1) {
								eventConsumer.restartBeforeNextPoll();
							}
						}
					}
					if (ex != null) {
						throw ex;
					}
				}
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void handleEvent(Event<Settings> event) { // new event from wf engine
		unsubscribeEvent();
		this.event = (E) event;
		subscribeData(event.fromTopic);
	}

	protected void post(List<O> data) {
		try {
			// TODO: Check what if data size is large
			producer.beginTransaction();
			for (Data obj : data) {
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

	private void subscribeData(String fromTopic) {

		Throwable ex = null;
		// TODO : handle while -> true
		while (true) {
			try {
				ConsumerRecords<String, Data> dataRecords = dataConsumer.poll();
				// TODO : Can below code go in consumer ??
				if (dataRecords.count() > 0) {
					for (TopicPartition partition : dataRecords.partitions()) {
						List<ConsumerRecord<String, Data>> partitionRecords = dataRecords.records(partition);
						if (partitionRecords.size() == 0) {
							continue;
						}
						if (logger.isInfoEnabled()) {
							logger.info("Got {} events between {} & {} in {}", partitionRecords.size(),
									partitionRecords.get(0).offset(),
									partitionRecords.get(partitionRecords.size() - 1).offset(), partition.toString());
						}

						List<Data> list = new ArrayList<>(partitionRecords.size());
						for (ConsumerRecord<String, Data> record : partitionRecords) {
							Data data = record.value();
							if (data == null) {
								continue;
							}
							// TODO : Expect only one event ?
							list.add(data);
						}

						try {

							if (list.size() > 0) {
								this.handleData(list);
							}

							if (partitionRecords.size() > 0) {
								long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
								eventConsumer.commitSync(
										Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
							}
						} catch (WakeupException | InterruptException e) {
							throw e;
						} catch (Throwable e) {
							logger.catching(e);
							if (ex == null) {
								ex = e;
							}
							try {
								logger.error("Seeking back {} events between {} & {} in {}", partitionRecords.size(),
										partitionRecords.get(0).offset(),
										partitionRecords.get(partitionRecords.size() - 1).offset(),
										partition.toString());

								eventConsumer.getConsumer().seek(partition, partitionRecords.get(0).offset());
							} catch (Throwable e1) {
								eventConsumer.restartBeforeNextPoll();
							}
						}
					}
					if (ex != null) {
						throw ex;
					}
				}
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
