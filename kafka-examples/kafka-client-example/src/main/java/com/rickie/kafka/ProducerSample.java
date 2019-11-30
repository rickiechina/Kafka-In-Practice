package com.rickie.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ProducerSample
{
    public static void main( String[] args ) throws ExecutionException, InterruptedException {
        System.out.println( "Hello World!" );
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "test_topic";
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        RecordMetadata metadata = producer.send(new ProducerRecord<String, String>
                (topic, "kafka", "Kafka v2.3 快速入门与实践"))
                .get();
        System.out.printf("partition=%d, offset=%d, metadata=%s%n",
                metadata.partition(), metadata.offset(), metadata);
        metadata = producer.send(new ProducerRecord<>
                (topic, "elasticsearch", "Elasticsearch 7.x从入门到精通"))
                .get();
        System.out.printf("partition=%d, offset=%d, metadata=%s%n",
                metadata.partition(), metadata.offset(), metadata);
        metadata = producer.send(new ProducerRecord<>
                (topic, "springcloud", "Spring Cloud Alibaba 微服务架构项目实战"))
                .get();
        System.out.printf("partition=%d, offset=%d, metadata=%s%n",
                metadata.partition(), metadata.offset(), metadata);
        producer.close();
    }
}
