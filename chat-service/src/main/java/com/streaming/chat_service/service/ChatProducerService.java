package com.streaming.chat_service.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.streaming.chat_service.dto.ChatMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ChatProducerService {
    public final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC = "live-chat-events";

    public void sendChat(ChatMessage chatMessage){
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TOPIC, chatMessage.getStreamId(), chatMessage);
        // CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TOPIC, chatMessage.getStreamId(), "{ bad_json: missing_quotes }");
        future.whenComplete((result, exception) -> {
            if(exception == null){
                log.info("Message sent successfully!");
                log.info("Message info: {}", result.getRecordMetadata().toString());
                log.info("Message info: {}", result.getProducerRecord().toString());
            }else{
                log.error("Can not sent message.");
            }
        });
    }
}
