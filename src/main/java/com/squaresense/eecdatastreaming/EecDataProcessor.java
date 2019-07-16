package com.squaresense.eecdatastreaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;

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

            KStream<Windowed<String>, EecData> group = input
                    .groupByKey(Serialized.with(new JsonSerde<>(String.class, mapper ), eecDataEventSerde))
                    .windowedBy(TimeWindows.of(SIZE_MS))
                    .aggregate(EecData::new,
                            (aggKey, eecDataEvent, aggEecData) -> aggEecData.aggregate(eecDataEvent))
            .toStream();
             group
                     .groupBy((stringWindowed, eecData) -> stringWindowed.key())
                     .reduce((eecData, eecData1) -> eecData.calculateMeanPower(),
                            Materialized.<String, EecData, KeyValueStore<Bytes, byte[]>>as("eec-data")
                                    .withKeySerde(Serdes.String()).
                                    withValueSerde(eecDataSerde));

        }
    }

    @StreamListener("input")
    public void calculateAverage(Flux<EecDataEvent> data) {
        data
                .window(Duration.ofSeconds(SIZE_MS))
                .flatMap(window -> window.groupBy(EecDataEvent::getMeter)
                        .flatMap(this::calculateAverage))
                .materialize();
    }

    private Mono<Average> calculateAverage(GroupedFlux<String, EecDataEvent> group) {
        return group
                .reduce(new Accumulator(0, BigDecimal.ZERO),
                        (a, d) -> new Accumulator(a.getCount() + 1, a.getTotalValue().add(d.getPower())))
                .map(accumulator -> new Average(group.key(), (accumulator.average())));
    }

    @Data
    @Builder
    static class Average {
        private String metter;
        private BigDecimal average;
        public Average(String metter, BigDecimal average) {
            this.metter = metter;
            this.average = average;
        }
    }

    @Data
    @Builder
    static class Accumulator {
        private int count;
        private BigDecimal totalValue;

        public Accumulator(int count, BigDecimal totalValue) {
            this.count = count;
            this.totalValue = totalValue;
        }
        public BigDecimal average(){
            return this.totalValue.divide(new BigDecimal(count),2, BigDecimal.ROUND_HALF_UP);
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

