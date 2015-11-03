package com.flipkart.redis.event.listener;

public interface AbstractEventListener<T> {

	public void onEvent(T event);
	
	public void onException(Throwable e);
}
