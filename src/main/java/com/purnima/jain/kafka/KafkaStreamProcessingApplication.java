package com.purnima.jain.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamProcessingApplication {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaStreamProcessingApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamProcessingApplication.class, args);
		logger.info("KafkaStreamProcessingApplication Started........");
	}

}