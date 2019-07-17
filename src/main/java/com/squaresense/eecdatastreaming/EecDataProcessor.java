package com.squaresense.eecdatastreaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
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

    public static void main(String[] args) {
        SpringApplication.run(EecDataProcessor.class, args);
    }

    @EnableBinding(KafkaStreamsProcessorX.class)
    public static class KafkaStreamsEecAggregatepplication {

        @StreamListener
        public void process(@Input("eec-input-data") KStream<String, EecDataEvent> input) {

            ObjectMapper mapper = new ObjectMapper();

            //KTable<Windowed<String>, EecData> aggregation =

            input
                    .groupByKey(Serialized.with(new JsonSerde<>(String.class, mapper ), new JsonSerde<>( EecDataEvent.class, mapper )))
                    .windowedBy(TimeWindows.of(SIZE_MS))
                    .aggregate(EecData::new,
                            (aggKey, eecDataEvent, aggEecData) -> aggEecData.aggregate(eecDataEvent))
                    .transformValues(KafkaStreamsEecAggregatepplication::get,
                            Materialized.<Windowed<String>, EecData, KeyValueStore<Bytes, byte[]>>as("eec-data")
                            .withKeySerde( new JsonSerde<>(Windowed.class, mapper))
                            .withValueSerde(new JsonSerde<>(EecData.class, mapper)))
            ;
        }

        private static ValueTransformerWithKey<Windowed<String>, EecData, EecData> get() {
            return new ValueTransformerWithKey<Windowed<String>, EecData, EecData>() {
                @Override
                public void init(ProcessorContext processorContext) {
                }
                @Override
                public EecData transform(Windowed<String> stringWindowed, EecData eecData) {
                    return eecData.updateEecMetrics();
                }
                @Override
                public void close() {
                }
            };
        }

    }

    @RestController
    public class EecDataController {

        @RequestMapping("/eec-data")
        public String events(String metter) {

            final ReadOnlyKeyValueStore<String, String> topFiveStore =
                    interactiveQueryService.getQueryableStore("eec-data", QueryableStoreTypes.<String, String>keyValueStore());
            return topFiveStore.get(metter);
        }
    }

    interface KafkaStreamsProcessorX {

        @Input("eec-input-data")
        KStream<?, ?> input();
    }
}

