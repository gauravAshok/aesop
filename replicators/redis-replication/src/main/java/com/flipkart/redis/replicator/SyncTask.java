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
//import com.flipkart.redis.event.CommandEvent;
//import com.flipkart.redis.event.EventHeader;
//import com.flipkart.redis.event.listener.BacklogEventListener;
//import com.flipkart.redis.net.Connection;
//import com.flipkart.redis.net.Reply;
//
///**
// * 
// * @author gaurav.ashok
// */
//
//class SyncTask implements Runnable {
//
//	private static final Logger logger = LoggerFactory.getLogger(SyncTask.class);
//	
//	Connection connection;
//	Subscriber<Reply<List<String>>> cmdEventSubscriber;
//	
//	public SyncTask(Connection connection, Subscriber<Reply<List<String>>> cmdEventSubscriber) {
//		this.connection = connection;
//		this.cmdEventSubscriber = cmdEventSubscriber;
//	}
//	
//	@Override
//	public void run() {
//		
//		if(connection != null && !connection.isConnected()) {
//			logger.debug("not connected to redis");
//			throw new RuntimeException("null subscriber provided.");
//		}
//		
//		if(cmdEventSubscriber == null) {
//			logger.debug("null subscriber provided.");
//			throw new RuntimeException("null subscriber provided.");
//		}
//		
//		ConnectableObservable<Reply<List<String>>> cmdEvents = connection.getCommands();
//
//		
//		cmdEvents.subscribe(cmdEventSubscriber);		
//		cmdEvents.connect();
//	}
//}
