package com.njit.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.njit.model.Messages;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Date;

@Service
public class KafkaSender {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    private Gson gson = new GsonBuilder().create();

    public void send() {
        Messages message = new Messages();
        message.setId(System.currentTimeMillis());
        message.setMsg("123");
        message.setSendTime(new Date());
        ListenableFuture<SendResult<String,String>> test0 = kafkaTemplate.send("newtopic",gson.toJson(message));
    }
}
