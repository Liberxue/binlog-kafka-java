package com.liberxue;

import com.liberxue.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    /**
     * @Title: Main
     * @ProjectName binlog-kafka-java
     * @Description: TODO
     * @author liberxue
     * @date 2019/11/423:12
     */

    private static final Logger log = LoggerFactory.getLogger( Main.class );


    public static void main(String[] args) {
        Consumer consumer = new Consumer( "test2", "127.0.0.1:9092", "test" );
        log.info("start consumer");
        consumer.start();
    }

}
