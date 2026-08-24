package com.streaming.chat_service.service;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import java.time.Instant;
import com.streaming.chat_service.dto.ChatMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ChatConsumerService {

    private final SimpMessagingTemplate messagingTemplate;
    
    @RetryableTopic(attempts = "3", dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = "${app.kafka.topic.chat}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeChatMessage(
            @Payload ChatMessage chatMessage,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp
        ) {
        // Convert the raw epoch milliseconds to IST
        String istTime = Instant.ofEpochMilli(timestamp)
                .atZone(java.time.ZoneId.of("Asia/Kolkata"))
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z"));

        // The exact 3 log lines requested
        log.info("--- [RECEIVED: {}] ---", istTime);
        log.info("Stream: {} | Sender: '{}' | Content: '{}'", chatMessage.getStreamId(), chatMessage.getSender(), chatMessage.getContent());
        // log.info("Kafka Metadata -> Partition: {} | Offset: {}", partition, offset); 
        
        messagingTemplate.convertAndSend("/topic/chat/" + chatMessage.getStreamId(), chatMessage);
    }

    @DltHandler
    public void handleChatMessageDLT(ChatMessage chatMessage, @Header(KafkaHeaders.ORIGINAL_TOPIC) String topic){
        log.warn("Message from topic {} is sent to DLT : the message is {}", topic, chatMessage.getContent());
    }
}
