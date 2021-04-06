package io.cloudio.task;

import java.util.List;

public abstract class OutputTask extends Task<Event, Data, Data>{

	OutputTask(String taskCode) {
		super(taskCode);
		
	}
	
	abstract void onData(Event E , List<Data> D);
	
	public void handleData(List<Data> data) {
		if (data.get(data.size() -1).isEnd()) {
			super.unsubscribeData();
		} else {
			this.onData(event, data);
		}
	}
}
