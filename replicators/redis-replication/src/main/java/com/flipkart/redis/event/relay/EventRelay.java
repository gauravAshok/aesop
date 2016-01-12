package com.flipkart.redis.event.relay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;

import com.flipkart.redis.event.listener.AbstractEventListener;

/*
 * Event Relay that subscribes to a stream of Type T and relay events to the event listener
 */
public class EventRelay<T> extends Subscriber<T>{

	private static final Logger logger = LoggerFactory.getLogger(EventRelay.class);
	
	AbstractEventListener<T> eventListener;
	
	public EventRelay(AbstractEventListener<T> listener) {
		this.eventListener = listener;
	}

	@Override
    public void onCompleted() {
	    logger.info("Events completed. Listener: {}", eventListener.getClass().getSimpleName());
    }

	@Override
    public void onError(Throwable e) {
		// only data exceptions thrown will be fatal resulting in replication thread death. 
		// In case of connection exception, have to retry re-establishing the connection.
		logger.error(e.getMessage());
		eventListener.onException(e);
    }

	@Override
    public void onNext(T t) {
		eventListener.onEvent(t);
    }
}
