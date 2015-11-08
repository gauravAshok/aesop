package com.flipkart.redis.event;

public class EventHeader {

	String masterId;
	long masterBacklogOffset;
	long timestamp;
	
	public EventHeader(String masterId, long masterBacklogOffset) {
		super();
		this.masterId = masterId;
		this.masterBacklogOffset = masterBacklogOffset;
		this.timestamp = System.currentTimeMillis();
	}
	
	public String getMasterId() {
		return masterId;
	}
	public long getMasterBacklogOffset() {
		return masterBacklogOffset;
	}
	public long getTimestamp() {
		return timestamp;
	}
}
