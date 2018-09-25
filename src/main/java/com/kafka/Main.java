package com.kafka;

/**
 * @Description: java类作用描述
 * @Author: HALEN(李智刚)
 * @CreateDate: 2018/9/321:59
 * <p>Copyright: Copyright (c) 2018</p>
 */
public class Main {
    public static void main(String[] args) {
       /* MyKafkaProducer myKafkaProducer = new MyKafkaProducer(KafkaProperties.TOPIC);
        new Thread(myKafkaProducer).start();*/
        MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer(KafkaProperties.TOPIC);
        new Thread(myKafkaConsumer).start();
    }
}
