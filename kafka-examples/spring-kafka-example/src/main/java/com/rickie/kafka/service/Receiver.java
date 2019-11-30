package com.rickie.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    //@KafkaListener(topics="${app.topic.test_topic}")
    public void listen(ConsumerRecord<?, ?> consumerRecord) {
        LOGGER.info("received key='{}', message='{}'", consumerRecord.key(), consumerRecord.value());
    }

    @KafkaListener(topics="${app.topic.test_topic}", groupId = "testGroup_2")
    public void listenBatch(List<ConsumerRecord<?, ?>> consumerRecordList) {
        LOGGER.info("正在批处理 ...消息条数：" + consumerRecordList.size());
        consumerRecordList.forEach(consumerRecord->{
            LOGGER.info("received key='{}', message='{}'", consumerRecord.key(), consumerRecord.value());
        });

    }
}
