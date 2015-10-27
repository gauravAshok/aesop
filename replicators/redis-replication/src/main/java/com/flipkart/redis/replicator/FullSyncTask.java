package com.flipkart.redis.replicator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;
import rx.observables.ConnectableObservable;

import com.flipkart.redis.event.BacklogEventListener;
import com.flipkart.redis.event.DataEvent;
import com.flipkart.redis.event.Datatype;
import com.flipkart.redis.net.Connection;
import com.flipkart.redis.net.rdb.RDBParser.Entry;

class FullSyncTask extends SyncTask {
	
	private static final Logger logger = LoggerFactory.getLogger(FullSyncTask.class);
	
	private static final Datatype[] int2DataTypeMapping = { 
    	Datatype.STRING, Datatype.LIST, Datatype.SET, Datatype.ZSET, 
    	Datatype.HASH, null, null, null, null, Datatype.HASH, Datatype.LIST, Datatype.SET,
    	Datatype.ZSET, Datatype.HASH
    };
	
	public FullSyncTask(Connection connection, BacklogEventListener listener, long masterbacklogOffset) {
		super(connection, listener, masterbacklogOffset);
	}
	
	@Override
	public void run() {
		
		ConnectableObservable<Entry> rdbEvents = connection.getRdbDump();
		rdbEvents.subscribe(new Subscriber<Entry>() {

            @Override
            public void onCompleted() {
                logger.info("rdb dump sync completed");
            }

            @Override
            public void onError(Throwable e) {
                eventListener.onException(e);
            }

			@Override
			public void onNext(Entry t) {
				DataEvent event = new DataEvent(t.key, t.value, t.database, int2DataTypeMapping[t.type]);
				
				eventListener.onEvent(event);
			}
		});
		
		rdbEvents.connect();
		
		// rdb stream ended, resume normal replication
		super.run();
	}
}
