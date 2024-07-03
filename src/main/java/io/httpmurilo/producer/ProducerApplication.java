package io.httpmurilo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@SpringBootApplication

public class ProducerApplication {

	private static final Logger logger = LoggerFactory.getLogger(ProducerApplication.class);

	private static final String TOPIC = "teste-entrada";
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";


	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		List<String> times = Arrays.asList("Santos", "São Paulo", "Flamengo", "Bahia", "Vitoria", "Palmeiras", "Red bull bragantino");

		times.forEach(palavra -> {
			ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, palavra);
			producer.send(record, (metadata, exception) -> {

				if (exception == null) {
					logger.info("Enviado record(value={}) para partition {} @ offset {}", palavra, metadata.partition(), metadata.offset());
				} else {
					logger.error("Erro ao enviar record", exception);
				}
			});
		});

		producer.close();
	}

}
