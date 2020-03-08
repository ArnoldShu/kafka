package com.arnold.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName TestConsumer
 * @Description: kafka消费者
 * @Author Arnold
 * @Date 2020/3/6
 * @Version V2.0
 **/
public class TestConsumer {
    public static void main(String[] args) throws InterruptedException {
        //配置消费者
        //kafka配置http://kafka.apache.org/0110/documentation.html#consumerconfigs
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop1:9092,hadoop2:9092,hadoop3:9092");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id","kafka-group");
        //创建消费者
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
        //订阅topic
        consumer.subscribe(Arrays.asList("hadoop1"));
        //死循环一致拉取消息
        while (true){
            ConsumerRecords<String, String> records =  consumer.poll(5000);
            if (records.isEmpty()){
                Thread.sleep(3000);
            }else {
                for (ConsumerRecord<String, String> obj:records) {
                        System.out.println("--------------------------------------------------");
                    System.out.println(obj);
                    Thread.sleep(10000);
                }
            }
        }
    }

}