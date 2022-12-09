package com.njit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.2.233:9092");
        properties.put("retries",1);
        properties.put("batch.size",16384);
        properties.put("linger.ms",1);
        properties.put("buffer.memory",33554432);
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i< 10; i++) {
            producer.send(new ProducerRecord<String, String>("study","Milo"+i));
        }
        producer.close();
    }
}
