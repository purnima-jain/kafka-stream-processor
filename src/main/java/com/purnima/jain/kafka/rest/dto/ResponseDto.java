package com.purnima.jain.kafka.rest.dto;

public class ResponseDto {

	private String message;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "ResponseDto [message=" + message + "]";
	}

}
