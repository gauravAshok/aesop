package com.flipkart.redis.event.generator;

import rx.Subscriber;

import com.flipkart.redis.event.EventHeader;
import com.flipkart.redis.event.listener.AbstractEventListener;
import com.flipkart.redis.replicator.state.ReplicatorState;

/*
 * Event Generator that subscribes to a stream of Type T and generates events of type U
 */
public abstract class AbstractEventGenerator<T, U> extends Subscriber<T>{

	AbstractEventListener<U> eventListener;
	
	ReplicatorState state;
	
	public AbstractEventGenerator(AbstractEventListener<U> listener, ReplicatorState state) {
		this.eventListener = listener;
		this.state = state;
	}
	
	protected EventHeader generateHeader(T event)
	{
		EventHeader header = new EventHeader(this.state.getMasterId(), this.state.getReplicationOffset());
		return header;
	}
}
