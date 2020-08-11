package com.purnima.jain.kafka.rest.dto;

public class UserDto {
	
	private Integer userId;
	private String firstName;
	private String lastName;

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	@Override
	public String toString() {
		return "UserRequestDto [userId=" + userId + ", firstName=" + firstName + ", lastName=" + lastName + "]";
	}
	
	public UserDto convertFirstNameToUpperCase() {
		UserDto userDto = new UserDto();
		userDto.setUserId(userId);
		userDto.setFirstName(firstName.toUpperCase());
		userDto.setLastName(lastName);
		return userDto;
	}

}
