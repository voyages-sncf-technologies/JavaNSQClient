package com.vsct.dt.nsq.exceptions;

public class NoConnectionsException extends NSQException {

	public NoConnectionsException(String message) {
		super(message);
	}

	public NoConnectionsException(String message, Throwable cause) {
		super(message, cause);
	}
}
