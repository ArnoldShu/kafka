package com.arnold.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @ClassName TestInterceptor
 * @Description: kafka拦截器(消息发送前,回调函数前),拦截加上数据时间戳以及加上统计消息发送成功或失败
 * @Author Arnold
 * @Date 2020/3/6
 * @Version V2.0
 **/
public class TestInterceptor implements ProducerInterceptor<String,String> {

    //成功变量
    private int sucess = 0;
    //失败变量
    private int fail = 0;
    //发送信息前逻辑加上时间戳
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String oldValue = producerRecord.value();
        String newValue = System.currentTimeMillis()+oldValue;
        return new ProducerRecord<String,String>(producerRecord.topic(),newValue);
    }

    //判断数据发送次数
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e==null){
            sucess++;
        }else {
            fail++;
        }
    }

    public void close() {
        System.out.println("消息发送成功次数:"+sucess);
        System.out.println("消息发送失败次数:"+fail);
    }

    public void configure(Map<String, ?> map) {

    }
}