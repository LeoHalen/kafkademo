package com.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Description: java类作用描述
 * @Author: HALEN(李智刚)
 * @CreateDate: 2018/9/320:58
 * <p>Copyright: Copyright (c) 2018</p>
 */
public class MyKafkaProducer implements Runnable{

    private final Producer<String, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public MyKafkaProducer(String topic){
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
//        props.put("zookeeper.connect", KafkaProperties.ZK);

        /*props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);*/

        props.put("key.serializer", KafkaProperties.StrSerializer);
        props.put("value.serializer", KafkaProperties.StrSerializer);
//        props.put("serializer.class", KafkaProperties.strDeserializer);
//        props.put("key.serializer.class", KafkaProperties.strDeserializer);
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        this.topic = topic;
    }
    public void run() {
        int messageNum = 1;
        while (true) {
            String messageStr = new String("Message_" + messageNum);
            System.out.println("Send:" + messageStr);
            producer.send(new ProducerRecord<String, String>(topic, messageStr));
            messageNum++;
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
