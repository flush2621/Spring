package com.njit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PartitionProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.2.233:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put("MyPartitioner.class","com.njit.MyPartitioner");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("study",Integer.toString(i), Integer.toString(i)),(recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("success ->"+
                            " partition = " + recordMetadata.partition() + " ~~~~~ offset = " + recordMetadata.offset());
                } else {
                    e.printStackTrace();
                }
            });
        }
        producer.close();
    }
}
