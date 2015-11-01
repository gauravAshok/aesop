//package com.flipkart.redis.replicator;
//
//import java.util.List;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import rx.Subscriber;
//import rx.observables.ConnectableObservable;
//
//import com.flipkart.redis.net.Connection;
//import com.flipkart.redis.net.Reply;
//import com.flipkart.redis.net.rdb.RDBParser.Entry;
//
//class FullSyncTask extends SyncTask {
//	
//	private static final Logger logger = LoggerFactory.getLogger(FullSyncTask.class);
//	
//	Subscriber<Entry> rdbDumpEntrySubscriber;
//	
//	public FullSyncTask(Connection connection, Subscriber<Entry> rdbDumpEntrySubscriber, 
//			Subscriber<Reply<List<String>>> cmdEventSubscriber) {
//		super(connection, cmdEventSubscriber);
//		this.rdbDumpEntrySubscriber = rdbDumpEntrySubscriber;
//	}
//	
//	public FullSyncTask(Connection connection, Subscriber<Entry> rdbDumpEntrySubscriber) {
//		this(connection, rdbDumpEntrySubscriber, null);
//	}
//	
//	@Override
//	public void run() {
//		
//		if(connection != null && !connection.isConnected()) {
//			logger.debug("not connected to redis");
//			throw new RuntimeException("not connected to redis");
//		}
//		if(rdbDumpEntrySubscriber == null) {
//			logger.debug("null subscriber provided.");
//			throw new RuntimeException("null subscriber provided.");
//		}
//		
//		ConnectableObservable<Entry> rdbEvents = connection.getRdbDump();
//		rdbEvents.subscribe(rdbDumpEntrySubscriber);
//		rdbEvents.connect();
//		
//		// rdb stream ended, resume normal replication, if subscriber is provided.
//		if(cmdEventSubscriber != null) {
//			super.run();
//		}
//	}
//}
