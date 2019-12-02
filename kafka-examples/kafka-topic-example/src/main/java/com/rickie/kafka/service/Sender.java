package com.rickie.kafka.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class Sender {
    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.topic.mytopic}")
    private String topic;

    public void send(String key, String message) {
        LOGGER.info("send message='{}' to topic='{}'", message, topic);

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("Unable to send message=["
                        + message + "] due to: " + throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                //System.out.println("success");
                RecordMetadata recordMetadata = result.getRecordMetadata();
                System.out.println("Sent message=[" + message + "] to topic=["
                        + recordMetadata.topic() + "] with offset=["
                        + recordMetadata.offset() +"] partition=["
                        + recordMetadata.partition() + "]");
            }
        });
    }
}
