package com.mbronshteyn;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.apachecommons.CommonsLog;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

@Log4j
public class BankAmountAppKey {

  public static void main(String[] args) {

    ObjectMapper objectMapper = new ObjectMapper();

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-application-java");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // enable exactly once
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    StreamsBuilder builder = new StreamsBuilder();

    // Create a stream to produce transaction records
    KStream<String, String> transactions = builder.stream("bank-input");

    // process headers
    transactions.process( () -> new Processor() {
        ProcessorContext context;
        @Override
        public void init(ProcessorContext context) {
          this.context = context;
        }
        @Override
        public void process(Object key, Object value) {
          final Header[] headers = this.context.headers().toArray();

          Transaction transaction = null;
          try {
            // convert json to java object
            transaction = objectMapper.readValue((String)value, Transaction.class);
          } catch (Exception e) {
            e.printStackTrace();
          }

          log.info( "Processing by header ************************");
          log.info( "Header key: "  + headers[0].key() + " ;Header value: " + new String( headers[0].value() ));
          log.info( "Transaction: " + transaction.toString() );
          log.info( "Processing by header ************************\n");

        }
        @Override
        public void close() {
        }
      });

      // do key based processing
      KStream<String, Transaction> transactionsStream = transactions
      .mapValues((key, value) -> {

        Transaction transaction;
        try {
          // convert json to java object
          transaction = objectMapper.readValue(value, Transaction.class);
        } catch (Exception e) {
          e.printStackTrace();
          return null;
        }
        return transaction;
      })
      .selectKey((key, value) -> value.getName() );

    transactionsStream.foreach((key, transaction) -> {
      log.info( "Processing by the key ===============================");
      log.info( "Key: " + key + "; Transaction: " + transaction.toString());
      log.info( "Processing by the key ===============================\n");
    });

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    // only do this in dev - not in prod
    streams.cleanUp();
    streams.start();

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
