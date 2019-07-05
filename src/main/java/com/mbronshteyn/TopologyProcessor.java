package com.mbronshteyn;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Log4j
public class TopologyProcessor {
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

        Topology builder = new Topology();

        builder.addSource( "Source", "bank-input");
        builder.addProcessor("Process", () -> new HeaderProcessor(), "Source");
        builder.addSink("Sink", "bank-output", "Process");

        KafkaStreams streams = new KafkaStreams(builder, config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
