package com.arnold.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @ClassName TestPartitioner
 * @Description: 自定义分区
 * @Author Arnold
 * @Date 2020/3/5
 * @Version V2.0
 **/
public class TestPartitioner implements Partitioner {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 1;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}