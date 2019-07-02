package org.apache.flink.streaming.scala.examples.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import org.apache.flink.api.scala._

object KakkaStreamingTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "flink-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest") //value 反序列化

    val myConsumer = new FlinkKafkaConsumer011[String]("flink-topic", new SimpleStringSchema(), props)
    val dataStreamSource: DataStream[String] = env.addSource(myConsumer).setParallelism(4)

    dataStreamSource.print()
    env.execute("Flink add data source")
  }
}
