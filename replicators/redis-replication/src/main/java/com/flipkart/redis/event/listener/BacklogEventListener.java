package com.flipkart.redis.event.listener;

import com.flipkart.redis.event.CommandEvent;
import com.flipkart.redis.event.DataEvent;

public interface BacklogEventListener {

	public void onEvent(CommandEvent event);
	
	public void onEvent(DataEvent event);
	
	public void onException(Throwable e);
}
