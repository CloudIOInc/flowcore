package io.cloudio.task;

import java.util.List;

public abstract class InputTask extends Task<Event, Data, Data>{

	InputTask(String taskCode) {
		super(taskCode);
	}

	
	abstract List<Data> onData(Event E , List<Data> D);
	
	public void handleData(List<Data> data) {
		if (data.get(data.size() -1).isEnd()) {
			super.unsubscribeData();
		} else {
			List<Data> output = this.onData(event, data);
			super.post(output);
		}
	}

}
