package com.rickie.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerSample {
    public static void main(String[] args) {
        String topic = "test_topic";
        Properties props = new Properties();
        // 指定kafka集群
        props.put("bootstrap.servers", "localhost:9092");
        // 指定消费者组
        props.put("group.id", "testGroup-01");
        // 开启自动提交
        props.put("enable.auto.commit", "true");
        // 自动提交的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // 反序列化消息key和value
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建一个KafkaConsumer对象
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅消费主题集合
        consumer.subscribe(Arrays.asList(topic));
        while(true) {
            // 获取topic消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record : records) {
                // 输出消息分区、偏移量、key和value
                System.out.printf("partition=%d, offset=%d, key=%s, value=%s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}
