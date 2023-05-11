package com.yongzh.springboot.springbootkafka.controller;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @author Administrator
 * @version 1.0
 * @program: springboot-kafka
 * @description:
 * @date 2023/5/12 0:00
 */
@Configuration
public class KafkaConsumer {

    @KafkaListener(topics = "first")
    public void consumerTopic(String msg){

        System.out.println("收到消息：" + msg);
    }
}
