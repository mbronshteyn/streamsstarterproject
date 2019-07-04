package com.mbronshteyn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.lang.annotation.ElementType;
import java.time.LocalDateTime;
import java.util.*;

public class Producer {

  public static void main(String[] args) throws Exception  {

    List<String> customers = Arrays.asList("John", "Dave", "Mike", "Anna", "Toby", "Lucy");

    // create producer propertes
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "producer-java");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()) ;
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );

    // set linger.ms to 10 ms to send 100 messages per second
    config.put(ProducerConfig.LINGER_MS_CONFIG, 10 );

    // create producer
    KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>( config );

    Random r = new Random();

    // create a ProducerRecord
    for( int i = 0; i < 100; i++ ){

      String name = customers.get(r.nextInt(customers.size()));

      // create data
      Transaction transaction = Transaction.builder()
          .name( name )
          .amount( r.nextInt( 100 ) )
          .timestamp(LocalDateTime.now().toString())
          .build();

      // sleep for .5 sec
      Thread.sleep( 500 );

      // send data
      // use name as key and transaction as data
      ProducerRecord<String,String > producerRecord =
        new ProducerRecord<>( "bank-input" , transaction.toJson()  );

      // add custom header
      producerRecord.headers().add( new RecordHeader("name", name.getBytes() ));

      kafkaProducer.send( producerRecord );
    }

    // flush the stream
    kafkaProducer.flush();
  }


}
