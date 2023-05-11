package com.yongzh.springboot.springbootkafka.controller;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Administrator
 * @version 1.0
 * @program: springboot-kafka
 * @description:
 * @date 2023/5/11 23:34
 */
@RestController
public class ProducerController {

    @Autowired
    KafkaTemplate<String,String> kafka;

    @RequestMapping("/wuhu")
     public String data(String msg){
         //通过kafka发送数据
        kafka.send("first",msg);

        return "ok";
     }
}
