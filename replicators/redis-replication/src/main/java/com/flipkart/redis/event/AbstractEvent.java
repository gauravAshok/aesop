package com.flipkart.redis.event;

/**
 * Abstract event containing master and backlog offset info.
 * @author gaurav.ashok
 */
public abstract class AbstractEvent {
	
	EventHeader header;
	
	public AbstractEvent(EventHeader header) {
		super();
		this.header = header;
	}

	public abstract String getKey();
	
	public EventHeader getHeader() {
		return header;
	}
}
