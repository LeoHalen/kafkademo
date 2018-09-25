package com.kafka;

/**
 * @Description: java类作用描述
 * @Author: HALEN(李智刚)
 * @CreateDate: 2018/9/320:36
 * <p>Copyright: Copyright (c) 2018</p>
 */
public class KafkaProperties {

    public static final  String ZK = "192.168.38.109:12181";
    public static final String TOPIC = "test2";
    public static final String BROKER_LIST = "192.168.38.109:9092";
    public static final String GROUP_ID = "test-consumer-group";
    public static final String StrSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String strDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
//    public static final String strDeserializer = "kafka.serializer.StringEncoder";
    public static final int kafkaServerPort = 9092;
    public static final int kafkaProducerBufferSize = 64 * 1024;
    public static final int connectionTimeOut = 20000;
    public static final int reconnectInterval = 10000;
    public static final String topic2 = "topic2";
    public static final String topic3 = "topic3";
    public static final String clientId = "SimpleConsumerDemoClient";
}
