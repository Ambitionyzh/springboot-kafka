package com.yongzh.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @author Administrator
 * @version 1.0
 * @program: springboot-kafka
 * @description:
 * @date 2023/5/12 21:00
 */
public class FlinkKafkaConsumer1 {
    public static void main(String[] args) throws Exception {
        // 0 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 1 kafka消费者配置信息
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        // 2 创建kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "first",
                new SimpleStringSchema(),
                properties
        );

        // 3 消费者和flink流关联
        env.addSource(kafkaConsumer).print();

        // 4 执行
        env.execute();


    }
}
