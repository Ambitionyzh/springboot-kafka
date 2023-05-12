package com.yongzh.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author Administrator
 * @version 1.0
 * @program: springboot-kafka
 * @description:
 * @date 2023/5/12 20:24
 */
public class FlinkKafkaProducer1 {
    public static void main(String[] args) throws Exception {
        //1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //2.准备数据源
        ArrayList<String> wordList = new ArrayList<>();
        wordList.add("hello");
        wordList.add("wuhu");
        DataStreamSource<String> stream = env.fromCollection(wordList);

        //创建一个kafka生产者
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");


        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("first",new SimpleStringSchema(),properties);


        //3.添加数据源
        stream.addSink(kafkaProducer);

        //4.执行代码
        env.execute();

    }
}
