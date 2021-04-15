package io.cloudio.task;
import java.util.HashSet;
import java.util.Set;

import io.cloudio.messages.Settings;

public class TransformSettings extends Settings{
	
	Set<String> fields = new HashSet<String>();

	public Set<String> getFields() {
		return fields;
	}

	public void setFields(Set<String> fields) {
		this.fields = fields;
	}
}
