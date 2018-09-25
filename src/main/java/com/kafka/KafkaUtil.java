package com.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaUtil{

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);

    //配置信息
    private Properties prop;

    // 话题名称
    private final String topic;

    // 线程数量，与kafka分区数量相同
    private final int threadNum;

    private int key = 0;

    public KafkaUtil(Properties prop) {

        this.prop = prop;
        topic = prop.getProperty("kafka.topic");
        threadNum = Integer.parseInt(prop.getProperty("thread.count"));

    }

    /**
     * 发送信息到kafka(key为null)
     */
    public void simpleAddQueue(String... msgs) {
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(prop));
        List<KeyedMessage<String, String>> data = new ArrayList<KeyedMessage<String, String>>();
        for (String msg : msgs) {
            data.add(new KeyedMessage<String, String>(topic, msg));
            LOG.debug("加入kafka队列:主题[" + topic + "];消息[" + msg + "]");
        }
        if (!data.isEmpty()) {
            producer.send(data);
            LOG.debug("发送kafka成功！");
        }
        // 关闭producer
        producer.close();
    }

    /**
     * 键值对形式发送消息到kafka
     */
    public void addQueue(Map<String, List<String>> msgs) {
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(prop));
        List<KeyedMessage<String, String>> data = new ArrayList<KeyedMessage<String, String>>();
        for (Entry<String, List<String>> entry : msgs.entrySet()) {
            for (String msg : entry.getValue()) {
                data.add(new KeyedMessage<String, String>(topic, entry.getKey(), msg));
                LOG.debug("加入kafka队列:主题[" + topic + "];key[" + entry.getKey() + "];消息[" + msg + "]");
            }
        }
        if (!data.isEmpty()) {
            producer.send(data);
            LOG.debug("发送kafka成功！");
        }
        producer.close();
    }

    /**
     * 根据threadNum平均发给每一个kafka分区
     */
    public void addQueue(String... msgs) {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        for (String msg : msgs) {
            key = key >= threadNum ? 0 : key;
            if (!map.containsKey(key + "")) {
                map.put(key + "", new ArrayList<String>());
            }
            map.get(key + "").add(msg);
            key++;
            if(key > Integer.MAX_VALUE/2){
                key = 0;
            }
        }
        addQueue(map);
    }

    /**
     * 获得默认的kafka消费流列表
     */
    public List<KafkaStream<byte[], byte[]>> getStream() {
        ConsumerConnector consumerConnector = Consumer
                .createJavaConsumerConnector(new ConsumerConfig(prop));

        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put(topic, threadNum);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
                .createMessageStreams(map);
        return consumerMap.get(topic);
    }

    /**
     * 根据groupId获得kafka消费流列表
     */
    public List<KafkaStream<byte[], byte[]>> getStream(String groupId) {
        prop.setProperty("group.id", groupId);
        return getStream();
    }

    /**
     * 获得话题
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 获得进程数，与kafka分区patition数相同
     */
    public int getThreadNum() {
        return threadNum;
    }

    public static void main(String args[]){
        /*System.out.println("开始发布...");
        testSendKfk();
        System.out.println("发布完成！");*/

        System.out.println("开始订阅...");
        testConsumer();
        System.out.println("订阅完成！");
        /*testGetStream();*/

    }

    /**
     * 测试发送
     */
    public static void testGetStream(){

        Properties conf = new Properties();
        conf.put("kafka.topic", "test_topic");
        conf.put("thread.count", "2");
        conf.put("zookeeper.connect", "192.168.38.109:12181");
        conf.put("zookeeper.connectiontimeout.ms", "30000");
        conf.put("zookeeper.session.timeout.ms", "800");
        conf.put("zookeeper.sync.time.ms", "200");
        conf.put("auto.commit.interval.ms", "1000");
        conf.put("auto.offset.reset", "smallest");
        conf.put("kafka.topic", "test_topic");
        conf.put("group.id", "test-consumer-group");

        KafkaUtil kfk = new KafkaUtil(conf);
        Iterator<KafkaStream<byte[], byte[]>> it = kfk.getStream("test-consumer-group").iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
    /**
     * 测试发送
     */
    public static void testSendKfk(){

        Properties conf = new Properties();
        conf.put("metadata.broker.list", "192.168.38.109:9092");
        conf.put("kafka.topic", "test_topic");
        conf.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("key.serializer.class", "kafka.serializer.StringEncoder");
        conf.put("thread.count", "2");

        KafkaUtil kfk = new KafkaUtil(conf);
        //for(int i=0;i<10;i++){
        kfk.addQueue("test1","test2","test3","test4","test5");
        kfk.addQueue("test1","test2","test3","test4","test5");
        kfk.addQueue("test1","test2","test3","test4","test5");
        kfk.addQueue("test1","test2","test3","test4","test5");
        kfk.addQueue("test1","test2","test3","test4","test5");
        LOG.info("发送完毕");
        //}
    }

    /**
     * 测试消费
     */
    public static void testConsumer(){

        Properties conf = new Properties();
        conf.put("kafka.topic", "test_topic");
        conf.put("thread.count", "2");
        conf.put("zookeeper.connect", "192.168.38.109:12181");
        conf.put("zookeeper.connectiontimeout.ms", "30000");
        conf.put("zookeeper.session.timeout.ms", "800");
        conf.put("zookeeper.sync.time.ms", "200");
        conf.put("auto.commit.interval.ms", "1000");
        conf.put("auto.offset.reset", "smallest");
        conf.put("kafka.topic", "test_topic");
        conf.put("group.id", "test-consumer-group");



        KafkaUtil kfk = new KafkaUtil(conf);

        List<KafkaStream<byte[], byte[]>> result = kfk.getStream();
        // 线程池
        ExecutorService executor = Executors.newFixedThreadPool(kfk.getThreadNum());
        for (final KafkaStream<byte[], byte[]> stream : result) {
            executor.submit(new ConsumerThread(stream));

        }
    }

}

