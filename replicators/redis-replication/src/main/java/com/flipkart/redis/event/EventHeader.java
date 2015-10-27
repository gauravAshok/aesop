package com.flipkart.redis.event;

public class EventHeader {

	String masterId;
	long masterBacklogOffset;
	
	public EventHeader(String masterId, long masterBacklogOffset) {
		super();
		this.masterId = masterId;
		this.masterBacklogOffset = masterBacklogOffset;
	}
	
	public String getMasterId() {
		return masterId;
	}
	public long getMasterBacklogOffset() {
		return masterBacklogOffset;
	}
}
