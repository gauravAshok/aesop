package com.flipkart.redis.event;

import com.flipkart.redis.event.data.AbstractData;

public class Event<EventData extends AbstractData> extends AbstractEvent {

	EventData data;
	
	public Event(EventData data, EventHeader header) {
	    super(header);
	    this.data = data;
    }

	@Override
    public String getKey() {
	    return data.getKey();
    }
	public EventData getData() {
		return data;
	}
}
