package com.squaresense.eecdatastreaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
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

    public static final int SIZE_MS = 1 * 3600 * 1000;
    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @EnableBinding(KafkaStreamsProcessorX.class)
    public static class KafkaStreamsAggregateSampleApplication {

        @StreamListener("input")
        public void process(KStream<String, EecDataEvent> input) {

            ObjectMapper mapper = new ObjectMapper();
            Serde<EecDataEvent> eecDataEventSerde = new JsonSerde<>( EecDataEvent.class, mapper );
            Serde<EecData> eecDataSerde = new JsonSerde<>(EecData.class, mapper);

            TimeWindowedKStream<String, EecDataEvent> grouped =   input
                    .groupByKey(Serialized.with(new JsonSerde<>(String.class, mapper ), eecDataEventSerde))
                    .windowedBy(TimeWindows.of(SIZE_MS));

            KTable<Windowed<String>, EecData> aggregate = grouped
                    .aggregate(EecData::new,
                            (aggKey, eecDataEvent, aggEecData) -> aggEecData.aggregate(eecDataEvent),
                            Materialized.<String, EecData, KeyValueStore<Bytes, byte[]>>as("eec-data")
                                    .withKeySerde(Serdes.String()).
                                    withValueSerde(eecDataSerde));
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

