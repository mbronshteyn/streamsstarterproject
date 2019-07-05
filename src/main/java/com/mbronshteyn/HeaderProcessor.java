package com.mbronshteyn;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

@Log4j
public class HeaderProcessor implements Processor<String, String> {

    ProcessorContext context;

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, String value ) {
        final Header[] headers = this.context.headers().toArray();

        Transaction transaction = null;
        try {
            // convert json to java object
            transaction = objectMapper.readValue( value, Transaction.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("Processing by header ************************");
        log.info("Header key: " + headers[0].key() + " ;Header value: " + new String(headers[0].value()));
        log.info("Transaction: " + transaction.toString());
        log.info("Processing by header ************************\n");
    }

    @Override
    public void close() {
    }
}
