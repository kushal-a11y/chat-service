package com.streaming.chat_service.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.streaming.chat_service.dto.ChatMessage;
import com.streaming.chat_service.service.ChatProducerService;

@RestController
@RequestMapping("/api/chat")
public class ChatController {

    private final ChatProducerService producerService;

    public ChatController(ChatProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/send")
    public ResponseEntity<String> sendChat(@RequestBody ChatMessage message) {
        producerService.sendChat(message);
        return ResponseEntity.ok("Chat message queued for processing!");
    }
}