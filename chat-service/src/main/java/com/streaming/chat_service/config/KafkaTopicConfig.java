package com.streaming.chat_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    
    @Value("${app.kafka.topic.chat}")
    private String KAFKA_TOPIC_NAME;

    @Bean
    public NewTopic liveChatTopic() {
        return TopicBuilder.name(KAFKA_TOPIC_NAME)
                .partitions(3)
                .replicas(3)
                .build();
    }
}
