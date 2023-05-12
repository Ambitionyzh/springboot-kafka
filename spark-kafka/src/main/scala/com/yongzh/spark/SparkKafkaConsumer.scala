package com.yongzh.spark
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer

object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    //初始化上下文环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-kafka");
    val ssc = new StreamingContext(conf, Seconds(3));


    //消费数据
    val kafkaPara = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "test"
    )
    val  KafkaSteam = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String, String](Set("first"), kafkaPara))

    val valueDSteam = KafkaSteam.map(record => record.value());
    valueDSteam.print();
    //执行代码
    ssc.start();
    ssc.awaitTermination();

  }
}
