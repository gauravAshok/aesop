package com.flipkart.redis.net;

public class Reply<T> {
	public T object;
	public int bytesRead;
	
	public Reply() {
	}
	
	public Reply(T object, int bytesRead) {
		this.object = object;
		this.bytesRead = bytesRead;
	}
}
