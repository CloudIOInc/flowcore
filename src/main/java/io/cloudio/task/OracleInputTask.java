
package io.cloudio.task;

import java.util.Collections;

import io.cloudio.consumer.EventConsumer;
import io.cloudio.messages.DBSettings;
import io.cloudio.messages.Settings;
import io.cloudio.producer.Producer;
import io.cloudio.util.GsonUtil;

public class OracleInputTask extends InputTask<Event<DBSettings>, Data> {
  private EventConsumer subTaskConsumer;
  private EventConsumer subTaskStatuseventConsumer;
  private Producer producer;

  private boolean isLeader = false;

  OracleInputTask(String taskCode) {
    super(taskCode);

  }

  public void start() {
    super.start();

    subTaskConsumer = new EventConsumer(groupId, Collections.singleton(eventTopic));
    subTaskStatuseventConsumer.createConsumer();
    subTaskStatuseventConsumer.subscribe();
    subscribeEvent(eventTopic);
  }

  @Override
  public void handleData(Event<DBSettings> event) {
    isLeader = true;
    DBSettings s = event.getSettings();
  }

  @Override
  protected Event<? extends Settings> getEvent(String eventJson) {
    return GsonUtil.getDBSettingsEvent(eventJson);
  }

}
