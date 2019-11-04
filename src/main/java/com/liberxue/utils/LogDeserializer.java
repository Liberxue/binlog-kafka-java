package com.liberxue.utils;

import com.liberxue.binlog.BinlogMessage.Binlog;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogDeserializer extends Adapter implements Deserializer<Binlog> {
    /**
     * @Title: LogDeserializer
     * @ProjectName binlog-kafka-java
     * @Description: TODO
     * @author liberxue
     * @date 2019/11/423:32
     */
    private static final Logger log = LoggerFactory.getLogger(LogDeserializer.class);

    @Override
    public Binlog deserialize(final String topic, byte[] data) {
        try {
            return Binlog.parseFrom(data);

        } catch (final InvalidProtocolBufferException e) {
            log.error("Received Unmarshaller message", e);
            throw new RuntimeException(
                    "Received Unmarshaller message " + e.getMessage(), e);
        }
    }
}


