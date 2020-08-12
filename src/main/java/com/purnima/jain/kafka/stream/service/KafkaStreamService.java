package com.purnima.jain.kafka.stream.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import com.purnima.jain.kafka.rest.dto.UserDto;

@Component
public class KafkaStreamService {
	
	@Value("#{kafkaConfig.getStreamInputTopicName()}")
	private String streamInputTopicName;
	
	@Value("#{kafkaConfig.getStreamOutputTopicName()}")
	private String streamOutputTopicName;
	
	@Bean
	public KStream<String, UserDto> kStream(StreamsBuilder kStreamBuilder) {
		KStream<String, UserDto> stream = kStreamBuilder.stream(streamInputTopicName, Consumed.with(Serdes.String(), new JsonSerde<>(UserDto.class)));
    	
		stream.print(Printed.<String, UserDto>toSysOut().withLabel("KafkaStreamProcessingApplication :: Before :: "));
		
		KStream<String, UserDto> upperCasedStream = stream.mapValues(userDto -> userDto.convertFirstNameToUpperCase());
		
		upperCasedStream.to(streamOutputTopicName, Produced.with(Serdes.String(), new JsonSerde<>(UserDto.class)));
		
		upperCasedStream.print(Printed.<String, UserDto>toSysOut().withLabel("KafkaStreamProcessingApplication :: After :: "));
        
        return upperCasedStream;
	}

}
