package com.authent.kafkaspring.handlers;


import com.authent.kafkaspring.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static jdk.vm.ci.code.CodeUtil.K;

@Component
public class PageEventHandler {
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (input)->{
            System.out.println("**********************");
            System.out.println(input.toString());
            System.out.println("**********************");
        };
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return ()->{
            return new PageEvent(
                    Math.random()>0.5?"P1":"P2",
                    Math.random()>0.5?"U1":"U2",
                    new Date(),
                    10+new Random().nextInt(10000)
            );
        };
    }


    @Bean
    public Function<KStream<String, PageEvent>,KStream<String,Long>>KStreamFunction() {
        return (input)->
                input.filter((K,v)->v.duration()>100)
                        .map((K,v)->new KeyValue<>(v.name(), v.duration()))
                        .groupByKey(Grouped.with(Serdes.String() ,Serdes.Long()))
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))) // le count aytadar gher f 5s lkhra
                        .count(Materialized.as("CountStore")) //le count
                        .toStream()
                        .map((k,v)->new KeyValue<>(k.key(),v))//pour que le retour soit coreespent a kstream
                ;

    }
    
    


}
