package com.flipkart.redis.net;

/**
 * Class to encapsulate object of type T and bytes read.
 * @author gaurav.ashok
 * @param <T>
 */
public class Reply<T> {
	public T object;
	public long bytesRead;
	
	public Reply() {
	}
	
	public Reply(T object, long bytesRead) {
		this.object = object;
		this.bytesRead = bytesRead;
	}
}
