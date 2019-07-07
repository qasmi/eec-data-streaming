package com.squaresense.eecdatastreaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class EecDataProcessor {

    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @EnableBinding(KafkaStreamsProcessorX.class)
    public static class KafkaStreamsAggregateSampleApplication {

        @StreamListener("input")
        public void process(KStream<String, EecDataEvent> input) {
            ObjectMapper mapper = new ObjectMapper();
            Serde<EecDataEvent> eecDataEventSerde = new JsonSerde<>( EecDataEvent.class, mapper );

            input
                    .groupByKey(Serialized.with(null, eecDataEventSerde))
                    .windowedBy(TimeWindows.of(1 *3600 * 1000))
                    .aggregate(
                            String::new,
                            (s, eecDataEvent, newEecData) -> null,
                            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("eec-data")
                                    .withKeySerde(Serdes.String()).
                                    withValueSerde(Serdes.String())
                    );
        }
    }

    @RestController
    public class EecDataController {

        @RequestMapping("/eec-data")
        public String events() {

            final ReadOnlyKeyValueStore<String, String> topFiveStore =
                    interactiveQueryService.getQueryableStore("eec-data", QueryableStoreTypes.<String, String>keyValueStore());
            return topFiveStore.get("12345");
        }
    }

    interface KafkaStreamsProcessorX {

        @Input("eec-input-data")
        KStream<?, ?> input();
    }
}

