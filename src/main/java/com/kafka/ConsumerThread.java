package com.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThread implements Runnable{



    private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

    private KafkaStream<byte[], byte[]> stream;

    public ConsumerThread(KafkaStream<byte[], byte[]> stream) {
        this.setStream(stream);
    }

    public void run() {
        try{
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while(it.hasNext()){
//                LOG.info((new String(it.next().message())));
                System.out.println("订阅：" + it.next().message());
            }
        }
        catch(Exception e){
            LOG.error("异常",e);
        }
    }
    public KafkaStream<byte[], byte[]> getStream() {
        return stream;
    }
    public void setStream(KafkaStream<byte[], byte[]> stream) {
        this.stream = stream;
    }

}