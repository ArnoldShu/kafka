package com.arnold.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName KafkaProducer
 * @Description: kafka生产者
 * @Author Arnold
 * @Date 2020/3/5
 * @Version V2.0
 **/
public class TestProducer {
    public static void main(String[] args) {
        //kafka配置http://kafka.apache.org/0110/documentation.html#producerconfigs
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop1:9092,hadoop2:9092,hadoop3:9092");
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("acks","1");
        //自定义分区
        properties.setProperty("partitioner.class","com.arnold.kafka.producer.TestPartitioner");
        //添加一个拦截器
        List<String> interceptorList = new ArrayList<String>();
        interceptorList.add("com.arnold.kafka.producer.TestInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptorList);

        //创建producer对象
        Producer<String,String> producer =new KafkaProducer<String,String>(properties);
        //组装数据
        String topic = "hadoop1";
        String key = "hadoop1";
        String value = "hadoop1";
        //指定分区插入数据
        //ProducerRecord record = new ProducerRecord(topic,1,key,value);
        ProducerRecord record = new ProducerRecord(topic,key,value);
        //发送数据
        producer.send(record, new Callback() {
            //异步回调函数
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.partition());
                System.out.println(recordMetadata.offset());
            }
        });
        //关闭
        producer.close();
    }
}