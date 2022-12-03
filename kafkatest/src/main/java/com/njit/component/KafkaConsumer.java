package com.njit.component;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaConsumer {

//    @Autowired
//    ConsumerFactory consumerFactory;
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory filterContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
//        factory.setConsumerFactory(consumerFactory);
//        factory.setAckDiscarded(true);
//        factory.setRecordFilterStrategy(consumerRecord -> {
//            if (Integer.parseInt(consumerRecord.value().toString()) % 2 == 0) {
//                return false;
//            }
//            return true;
//        });
//        return factory;
//    }
//
//    @KafkaListener(topics = {"topic1"},containerFactory = "filterContainerFactory")
//    public void onMessage6(ConsumerRecord<?,?> record) {
//        System.out.println(record.value());
//    }

    @KafkaListener(topics={"topic1"})
    public void onMessage(ConsumerRecord<?,?> record) {
        System.out.println("Simple Consumer: "+record.topic()+"-"+record.partition()+"-"+record.value());
    }

    @KafkaListener(id="consumer1",groupId = "felix-group",topicPartitions = {
            @TopicPartition(topic = "topic1",partitions = {"0"}),
            @TopicPartition(topic = "topic2",partitions = {"0"},partitionOffsets =
            @PartitionOffset(partition = "1",initialOffset = "8"))
    })
    public void onMessage2(ConsumerRecord<?,?> record) {
        System.out.println("topic:"+record.topic()+"|partition:"+record.partition()
                +"|offset:"+record.offset()+"|value:"+record.value());
    }

    @KafkaListener(id = "consumer2",groupId = "felix-group",topics = "topic1")
    public void onMessage3(List<ConsumerRecord<?,?>> records) {
        System.out.println(">>>批量消费一次,records.size()="+records.size());
        for (ConsumerRecord<?,?> record:records) {
            System.out.println(record.value());
        }
    }

    @KafkaListener(topics = "topic1",errorHandler = "consumerAwareErrorHandler")
    public void onMessage4(ConsumerRecord<?,?> record) throws Exception {
        throw new Exception("简单消费-模拟异常");
    }

    @KafkaListener(topics = "topic1",errorHandler = "consumerAwareErrorHandler")
    public void onMessage5(List<ConsumerRecord<?,?>> records) throws Exception {
        System.out.println("批量消费异常");
        throw new Exception("批量消费-模拟异常");
    }
}
