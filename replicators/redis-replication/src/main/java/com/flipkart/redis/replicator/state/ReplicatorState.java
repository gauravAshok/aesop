package com.flipkart.redis.replicator.state;

public class ReplicatorState {

	private String masterId;
	private long replicationOffset;
	
	public ReplicatorState(String masterId, long replicationOffset) {
		super();
		this.masterId = masterId;
		this.replicationOffset = replicationOffset;
	}

	public long getReplicationOffset() {
		return replicationOffset;
	}

	public void setReplicationOffset(long replicationOffset) {
		this.replicationOffset = replicationOffset;
	}

	public String getMasterId() {
		return masterId;
	}
}
