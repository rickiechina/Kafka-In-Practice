package com.rickie.kafka.async;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class ProducerAsync {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        String topic = "test_topic";
        String key = "";
        String value = "";
        Map<String, String> messages = new HashMap<>();
        messages.put("kafka", "kafka v2.3 tutorial and practice");
        messages.put("elasticsearch", "Elasticsearch 7.x from beginner to master");
        messages.put("springcloud", "Spring Cloud Alibaba in Practice");

        Iterator iterator = messages.keySet().iterator();
        while(iterator.hasNext()) {
            key = (String) iterator.next();
            value = messages.get(key);
            // 实例化一个消息记录对象，用来保存topic,key,value等等
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    topic, key, value);
            // 调用send() 方法和回调函数
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.println("offset: " + recordMetadata.offset());
                    }
                }
            });
        }

        // 优先把消息处理完毕，优雅退出。
        kafkaProducer.close();
    }
}
