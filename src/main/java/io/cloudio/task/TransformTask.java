package io.cloudio.task;

import java.util.List;

public abstract class TransformTask extends Task<Event, Data, Data> {

	public TransformTask(String taskCode) {
		super(taskCode);

	}

	public abstract List<Data> onData(Event<TransformSettings> E, List<Data> D);

	public void handleData(List<Data> data) {
		if (data.get(data.size() - 1).isEnd()) {
			super.unsubscribeData();
		} else {
			List<Data> output = this.onData(event, data);
			super.post(output);
		}
	}
}
