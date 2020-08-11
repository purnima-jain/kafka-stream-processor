package com.purnima.jain.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;


import com.purnima.jain.kafka.rest.dto.UserDto;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsConfig.class);
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;
	
	@Value("${spring.kafka.stream.application-id}")
	private String applicationId;

	@Value("#{kafkaConfig.getStreamInputTopicName()}")
	private String streamInputTopicName;
	
	@Value("#{kafkaConfig.getStreamOutputTopicName()}")
	private String streamOutputTopicName;
	
	@SuppressWarnings("resource")
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kStreamsConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new JsonSerde<>(UserDto.class).getClass());
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		return new KafkaStreamsConfiguration(props);
	}

	@Bean
	public KStream<String, UserDto> kStream(StreamsBuilder kStreamBuilder) {
		logger.info("Entering KafkaStreamsConfig.kStream()............");
		KStream<String, UserDto> stream = kStreamBuilder.stream(streamInputTopicName, Consumed.with(Serdes.String(), new JsonSerde<>(UserDto.class)));
    	
		stream.print(Printed.toSysOut());
		stream.mapValues(userDto -> userDto.convertFirstNameToUpperCase())
				.to(streamOutputTopicName, Produced.with(Serdes.String(), new JsonSerde<>(UserDto.class)));
        
		logger.info("Leaving KafkaStreamsConfig.kStream()............");
        return stream;
	}

}
