package com.flipkart.redis.exception;

public class ReplicatorRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 6757019054690431877L;

	public ReplicatorRuntimeException(String message) {
		super(message);
	}

	public ReplicatorRuntimeException(Throwable e) {
		super(e);
	}

	public ReplicatorRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}
}
