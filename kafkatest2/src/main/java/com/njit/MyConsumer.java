package com.njit;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class MyConsumer {

    private static Map<TopicPartition,Long> currentOffset = new HashMap<>();
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.2.233:9092");
        props.put("group.id","test");
        props.put("enable.auto.commit","false");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                commitOffset(currentOffset);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                currentOffset.clear();
                for (TopicPartition partition : collection) {
                    consumer.seek(partition,getOffset(partition));
                }
            }
        });
    }
    // 获取某分区的最新 offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }
    // 提交该消费者所有分区的 offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
