
package com.demo.controllers;

import java.net.URI;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.demo.events.TaskEventProducer;
import com.demo.messages.Record;
import com.demo.util.Util;
import com.google.gson.JsonObject;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {

  private final TaskEventProducer producer;

  @Autowired
  KafkaController(TaskEventProducer producer) {
    this.producer = producer;
  }

  @PostMapping(value = "/publish", consumes = { MediaType.APPLICATION_JSON_VALUE,
      MediaType.APPLICATION_XML_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE,
          MediaType.APPLICATION_XML_VALUE })
  public ResponseEntity<String> publish(@RequestBody String request) throws Exception {
    Record r = Util.getRecord(request);
    this.producer.sendMessage(r);
    JsonObject response = new JsonObject();
    response.addProperty("response", "OK");
    response.addProperty("status", "Event posted successfully!");
    return ResponseEntity
        .created(URI
            .create("/publishdata"))
        .body(response.toString());

  }
}
