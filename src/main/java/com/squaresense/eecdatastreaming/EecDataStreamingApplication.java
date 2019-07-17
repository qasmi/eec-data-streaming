package com.squaresense.eecdatastreaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class EecDataStreamingApplication {

	public static void main(String[] args) {
		ObjectMapper mapper = new ObjectMapper();
		Serde<EecDataEvent> eecDataEventSerde = new JsonSerde<>(EecDataEvent.class, mapper);


		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, eecDataEventSerde.serializer().getClass());

		EecDataEvent eecDataEvent = new EecDataEvent();
		eecDataEvent.setMeter("12345");
		eecDataEvent.setDate(Instant.now().getEpochSecond());
		eecDataEvent.setEnergy(BigDecimal.TEN);
		eecDataEvent.setPower(BigDecimal.ONE);

		DefaultKafkaProducerFactory<String, EecDataEvent> pf = new DefaultKafkaProducerFactory<>(props);
		KafkaTemplate<String, EecDataEvent> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("eec-input-data");

		template.sendDefault(eecDataEvent.getMeter(), eecDataEvent);
	}
}
