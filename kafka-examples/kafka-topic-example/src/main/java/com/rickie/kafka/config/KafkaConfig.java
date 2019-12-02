package com.rickie.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
    @Value("${app.topic.mytopic}")
    private String topic;

    @Bean
    public NewTopic topic() {
        return new NewTopic(topic, 3, (short)1);
    }
}
