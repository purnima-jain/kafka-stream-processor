package com.purnima.jain.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
	
	@Value("${spring.kafka.stream.input.topic-name}")
	private String streamInputTopicName;
	
	@Value("${spring.kafka.stream.output.topic-name}")
	private String streamOutputTopicName;
	
	@Bean
	public String getStreamInputTopicName() {
	    return streamInputTopicName;
	}
	
	@Bean
	public String getStreamOutputTopicName() {
	    return streamOutputTopicName;
	}

}
