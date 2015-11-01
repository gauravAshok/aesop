package com.flipkart.redis.event.generator;

import rx.Subscriber;

import com.flipkart.redis.event.EventHeader;
import com.flipkart.redis.event.listener.BacklogEventListener;
import com.flipkart.redis.replicator.state.ReplicatorState;

public abstract class AbstractEventGenerator<T> extends Subscriber<T>{

	BacklogEventListener eventListener;
	
	ReplicatorState state;
	
	public AbstractEventGenerator(BacklogEventListener listener, ReplicatorState state) {
		this.eventListener = listener;
		this.state = state;
	}
	
	protected EventHeader generateHeader(T event)
	{
		EventHeader header = new EventHeader(this.state.getMasterId(), this.state.getReplicationOffset());
		return header;
	}
}
