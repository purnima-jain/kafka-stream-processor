package com.purnima.jain.kafka.stream.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import com.purnima.jain.kafka.rest.dto.UserDto;

@Component
public class KafkaStreamService {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaStreamService.class);
	
	@Value("#{kafkaConfig.getStreamInputTopicName()}")
	private String streamInputTopicName;
	
	@Value("#{kafkaConfig.getStreamOutputTopicName()}")
	private String streamOutputTopicName;
	
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
