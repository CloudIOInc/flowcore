
package com.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import com.demo.messages.KafkaUtil;

@SpringBootApplication
@EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
public class SpringBootWithKafkaApplication {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(SpringBootWithKafkaApplication.class, args);
    KafkaUtil.start();
  }
}
