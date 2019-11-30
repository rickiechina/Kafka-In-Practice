package com.rickie.kafka;

import com.rickie.kafka.service.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaApp implements CommandLineRunner
{
    private static Logger LOGGER = LoggerFactory.getLogger(SpringKafkaApp.class);

    public static void main( String[] args )
    {
        LOGGER.info("SpringApplication start to run ...");
        SpringApplication.run(SpringKafkaApp.class, args);
        LOGGER.info("SpringApplication has started.");
    }

    @Autowired
    private Sender sender;

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("send message ...");
        String key = "";
        for(int i=0; i<20; i++) {
            key = "spring-kafka-" + i;
            sender.send(key, "Spring kafka producer and consumer example.");
        }
    }
}
