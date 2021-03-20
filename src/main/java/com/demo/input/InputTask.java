
package com.demo.input;

import com.demo.messages.TaskEvent;

public interface InputTask {

  String getSQL();

  void execute(TaskEvent message);
}
