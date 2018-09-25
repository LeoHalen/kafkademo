package com.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @Description: java类作用描述
 * @Author: HALEN(李智刚)
 * @CreateDate: 2018/9/321:14
 * <p>Copyright: Copyright (c) 2018</p>
 */
public class MyKafkaConsumer implements Runnable{

    private final Consumer<String, String> consumer;
    private final String topic;
    private final Properties props = new Properties();

    public MyKafkaConsumer(String topic) {
//        props.put("zookeeper.connect", KafkaProperties.ZK);
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        props.put("group.id", KafkaProperties.GROUP_ID);
        props.put("key.deserializer", KafkaProperties.strDeserializer);
        props.put("value.deserializer", KafkaProperties.strDeserializer);
       /* props.put("zookeeper.seesion.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");*/
//        props.put("session.timeout.ms", "30000");
        consumer = new KafkaConsumer<String, String>(props);
        this.topic = topic;
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void run() {
        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(100);
            /*for (ConsumerRecord<String, String> record : records) {
                System.out.println("receive:" + record.value());
            }*/
                for (ConsumerRecord<String, String> record : records) {
                    //这个有点好，可以直接通过record.offset()得到offset的值
    //            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }

    }
}
