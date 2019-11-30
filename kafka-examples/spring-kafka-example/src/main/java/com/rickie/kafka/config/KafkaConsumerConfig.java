package com.rickie.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 消费者分组ID，分组内的消费者只能消费该消息一次，不同分组内的消费者可以重复消费该消息。
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup_1");
        // latest ：当各分区下有已提交的offset时，从提交的offset开始消费；
        // 无提交的offset时，消费新产生的该分区下的数据
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // session.timeout.ms：检测消费者故障的超时，默认30000ms
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        // 自动提交位移
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 设置批量消费每次最多消费多少条消息记录
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);

        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
            kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // 消费监听器容器并发数
        factory.setConcurrency(3);
        // 设置批量消费
        factory.setBatchListener(true);
        //要设置监听容器的属性，比如轮询时间，只能是通过
        // getContainerProperties().set()方法，来设置。
        factory.getContainerProperties().setPollTimeout(3000);

        return factory;
    }
}
