package com.flipkart.redis.event;

public interface BacklogEventListener {

	public void onEvent(CommandEvent event);
	
	public void onEvent(RDBDataEvent event);
	
	public void onException(Throwable e);
}
