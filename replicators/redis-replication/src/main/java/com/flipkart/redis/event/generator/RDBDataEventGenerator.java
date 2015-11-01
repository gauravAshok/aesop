package com.flipkart.redis.event.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.redis.event.DataEvent;
import com.flipkart.redis.event.EventHeader;
import com.flipkart.redis.event.listener.BacklogEventListener;
import com.flipkart.redis.net.Datatype;
import com.flipkart.redis.net.rdb.RDBParser.Entry;
import com.flipkart.redis.replicator.state.ReplicatorState;

public class RDBDataEventGenerator extends AbstractEventGenerator<Entry>{

	private static final Logger logger = LoggerFactory.getLogger(RDBDataEventGenerator.class);

	private static final Datatype[] int2DataTypeMapping = { 
		Datatype.STRING, Datatype.LIST, Datatype.SET, Datatype.ZSET, 
		Datatype.HASH, null, null, null, null, Datatype.HASH, Datatype.LIST, Datatype.SET,
		Datatype.ZSET, Datatype.HASH
    };
	
	public RDBDataEventGenerator(BacklogEventListener listener, ReplicatorState state) {
		super(listener, state);
	}

	@Override
	public void onCompleted() {
		logger.info("RDBDataEvents have ended.");
	}

	@Override
	public void onError(Throwable e) {
		eventListener.onException(e);
	}

	@Override
	public void onNext(Entry t) {
		
		DataEvent event = new DataEvent(t.key, t.value, int2DataTypeMapping[t.type], t.database, this.generateHeader(t));
		
		eventListener.onEvent(event);
	}
	
	@Override
	protected EventHeader generateHeader(Entry t) {
		EventHeader header = new EventHeader(this.state.getMasterId(), t.streamOffset);
		return header;
	}
}
