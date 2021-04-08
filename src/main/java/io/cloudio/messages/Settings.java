
package io.cloudio.messages;

import java.util.HashSet;
import java.util.Set;

public class Settings {

	private String type;
	private Set<String> fields = new HashSet<String>();

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Set<String> getFields() {
		return fields;
	}

	public void setFields(Set<String> fields) {
		this.fields = fields;
	}

}
