
package io.cloudio.task;

import java.util.List;

import io.cloudio.messages.OutputSettings;
import io.cloudio.messages.TaskRequest;

public abstract class OutputTask extends Task<TaskRequest<OutputSettings>, Data, Data> {

  public OutputTask(String taskCode) {
    super(taskCode);

  }

  public abstract void onData(TaskRequest<OutputSettings> E, List<Data> D);

  public void handleData(List<Data> data) {
    if (data.get(data.size() - 1).isEnd()) {
      super.unsubscribeData();
    } else {
      this.onData(event, data);
    }
  }
}
