package com.liberxue.consumer;

import com.liberxue.utils.LogDeserializer;
import com.liberxue.utils.Unmarshaller;
import com.liberxue.binlog.BinlogMessage.Binlog;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class Consumer {
    /**
     * @Title: consumer
     * @ProjectName 0.0.1
     * @Description: Kafka Consumer
     * @author liberxue
     * @date 2019/8/2710:11
     */
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final KafkaConsumer<Integer, Binlog> consumer;

    private final String topic;

    public Consumer(String topic,String broker,String group){
        Properties props =new Properties(  );
        // consumer  broker  address config
        props.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,broker );
        // consumer group config
        props.put(ConsumerConfig.GROUP_ID_CONFIG,group);
        // auto commit status config
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        // auto commit interval messages config ps :ms
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        // session timeout config ps: ms
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,3000);
        consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new LogDeserializer());
        this.topic=topic;
    }

    public void start() {
        while (true) {
            consumer.subscribe( Collections.singletonList( this.topic ) );
            ConsumerRecords<Integer, Binlog> records = consumer.poll( 1000 );
            for (ConsumerRecord<Integer, Binlog> record : records) {
                JsonFormat.printToString( record.value() );
                // ProtoBuf to json reference https://code.google.com/archive/p/protobuf-java-format/
                log.info( JsonFormat.printToString( record.value() ) );
                String JsonData = JsonFormat.printToString( record.value() );
                System.out.println( JsonData );
                new Unmarshaller( JsonData );
            }

        }
    }
}
