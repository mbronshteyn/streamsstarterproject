package com.mbronshteyn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest" );
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    // 1 stream from Kafka
    KStream<String, String> wordCountInput = builder.stream("word-count-input");

    // 2 map values to lower case
    KTable<String, Long> wordCounts = wordCountInput
      .mapValues(value -> value.toLowerCase())
      .flatMapValues(value -> Arrays.asList(value.split(" ")))
      .selectKey((ignoredKey, value) -> value)
      .groupByKey()
      .count();

    wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    KafkaStreams kafkaStreams = new KafkaStreams( builder.build(), config  );
    kafkaStreams.start();

    // print topology
    System.out.println( kafkaStreams.toString() );

    // shutdown stream gracefully
    Runtime.getRuntime().addShutdownHook( new Thread( kafkaStreams::close ));


  }
}
