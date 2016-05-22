package com.ts.exception;

public class BackendException extends Exception {

	private static final long serialVersionUID = 8454576074694018441L;

	private String msg;
	
	public BackendException(String msg) {
		 this.msg = msg;
	}
	
	public String getBackendMessage() {
		return msg;
	}
	
}
