package com.njit.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaProducer {
    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    @GetMapping("/kafka/normal/{message}")
    public void sendMessage1(@PathVariable("message") String normalMessage) {
        kafkaTemplate.send("topic1", normalMessage);
    }

    @GetMapping("/kafka/callbackOne/{message}")
    public void sendMessage2(@PathVariable("message") String callbackMessage) {
        kafkaTemplate.send("topic1",callbackMessage).addCallback(success -> {
            String topic = success.getRecordMetadata().topic();
            int partition = success.getRecordMetadata().partition();
            long offset = success.getRecordMetadata().offset();
            System.out.println("Send Message Success."+topic+"-"+partition+"-"+offset);
        }, failure -> {
            System.out.println("Send message failure."+failure.getMessage());
        });
    }

    @GetMapping("/kafka/callbackTwo/{message}")
    public void sendMessage3(@PathVariable("message") String callbackMessage) {
        kafkaTemplate.send("topic1",callbackMessage).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Send Message Failure."+ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                System.out.println("Send Message Success."+result.getRecordMetadata().topic()+"-"
                +result.getRecordMetadata().partition()+"-"+result.getRecordMetadata().offset());
            }
        });
    }

    @GetMapping("/kafka/transaction")
    public void sendMessage7() {
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send("topic1","test executeInTransaction");
            throw new RuntimeException("fail");
        });
        kafkaTemplate.send("topic1","test executeInTransaction");
        throw new RuntimeException("fail");
    }

}
