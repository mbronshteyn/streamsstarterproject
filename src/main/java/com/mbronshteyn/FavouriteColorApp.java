package com.mbronshteyn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class FavouriteColorApp {

  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest" );
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    // 1 interStream from Kafka
    KStream<String, String> colorInput = builder.stream("color-input");

    KStream<String, String> interStream = colorInput
      // 2 map values to lower case
      .filter( ( key, value ) ->  value.contains( "," ))
      // 3 - select key
      .selectKey( ( ignoredKey, value ) ->  value.split( ",")[0] )
      // 4 - map values
      .mapValues( value -> value.split(",")[1].toLowerCase() )
      // 5 - remove bad colors
      .filter( ( key, value ) -> {
        return  value.equals( "red") || value.equals( "blue") || value.equals( "green");
      });

    // 6 - write to intermediate topic
    interStream.to("color-input-keyed", Produced.with(Serdes.String(), Serdes.String()));

    // 7 - read from intermideate stream into KTable
    KTable<String,String> keyedInput = builder.table( "color-input-keyed");

    // 8 - group by colors
    KTable<String, Long> colorCount = keyedInput
      .groupBy( ( key, value ) -> new KeyValue( value, value ))
      .count( );

    // 9  - write to final Kafak topic
    colorCount.toStream().to("color-output", Produced.with(Serdes.String(), Serdes.Long()));

    KafkaStreams kafkaStreams = new KafkaStreams( builder.build(), config  );
    kafkaStreams.start();

    // print topology
    System.out.println( kafkaStreams.toString() );

    // shutdown interStream gracefully
    Runtime.getRuntime().addShutdownHook( new Thread( kafkaStreams::close ));

  }
}
