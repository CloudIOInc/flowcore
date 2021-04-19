package io.cloudio.task;

import java.util.List;

import io.cloudio.messages.OutputSettings;

public abstract class OutputTask extends Task<Event<OutputSettings>, Data, Data>{

	public OutputTask(String taskCode) {
		super(taskCode);
		
	}
	
	public abstract void onData(Event E , List<Data> D);
	
	public void handleData(List<Data> data) {
		if (data.get(data.size() -1).isEnd()) {
			super.unsubscribeData();
		} else {
			this.onData(event, data);
		}
	}
}
