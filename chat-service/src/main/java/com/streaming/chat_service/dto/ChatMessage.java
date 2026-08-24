package com.streaming.chat_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChatMessage{

    private String streamId;
   
    private String sender;
   
    private String content;
}