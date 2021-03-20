
package com.demo.events;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.demo.messages.Topics;

@Service
public class TaskEventProducer {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  public void sendMessage(String message) {
    this.kafkaTemplate.send(Topics.EVENT_TOPIC, message);
  }
}
