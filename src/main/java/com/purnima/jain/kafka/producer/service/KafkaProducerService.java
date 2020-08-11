package com.purnima.jain.kafka.producer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.purnima.jain.kafka.rest.dto.UserDto;

@Service
public class KafkaProducerService {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

	private KafkaTemplate<String, UserDto> kafkaTemplate;
	
	@Value("#{kafkaConfig.getStreamInputTopicName()}")
	private String streamInputTopicName;
	
	@Autowired
	KafkaProducerService(KafkaTemplate<String, UserDto> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void sendMessage(UserDto userDto) {

		ListenableFuture<SendResult<String, UserDto>> future = kafkaTemplate.send(streamInputTopicName, String.valueOf(userDto.getUserId()), userDto);

		future.addCallback(new ListenableFutureCallback<SendResult<String, UserDto>>() {

			@Override
			public void onSuccess(SendResult<String, UserDto> result) {
				logger.info("Sent message=[" + userDto + "] with offset=[" + result.getRecordMetadata().offset() + "]");
			}

			@Override
			public void onFailure(Throwable ex) {
				logger.info("Unable to send message=[" + userDto + "] due to : " + ex.getMessage());
			}
		});
	}

}
