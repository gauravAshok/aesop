package com.flipkart.redis.replicator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;
import rx.observables.ConnectableObservable;

import com.flipkart.redis.event.BacklogEventListener;
import com.flipkart.redis.event.CommandEvent;
import com.flipkart.redis.event.EventHeader;
import com.flipkart.redis.net.Connection;
import com.flipkart.redis.net.Reply;

/**
 * 
 * @author gaurav.ashok
 */

class SyncTask implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(SyncTask.class);
	
	Connection connection;
	BacklogEventListener eventListener;
	
	String masterId;
	long masterBacklogOffset;
	
	public SyncTask(Connection connection, BacklogEventListener listener, String masterId, long masterbacklogOffset) {
		this.connection = connection;
		this.eventListener = listener;
		this.masterBacklogOffset = masterbacklogOffset;
	}
	
	@Override
	public void run() {
		ConnectableObservable<Reply<List<String>>> cmdEvents = connection.getCommands();
		cmdEvents.subscribe(new Subscriber<Reply<List<String>>>() {

            @Override
            public void onCompleted() {
            	logger.info("replication has ended.");
            }

            @Override
            public void onError(Throwable e) {
                eventListener.onException(e);
            }

			@Override
			public void onNext(Reply<List<String>> cmd) {
				
				masterBacklogOffset += cmd.bytesRead;
				
				// if command is ping, send the replication ack
				if(cmd.object.get(0).equals("PING")) {
					logger.debug("PING: replconf ack " + masterBacklogOffset);
					connection.sendReplAck(masterBacklogOffset);
				}
				else {
					List<String> args = cmd.object.subList(2, cmd.object.size());
					
					EventHeader header = new EventHeader(masterId, masterBacklogOffset);
					CommandEvent cmdEvent = new CommandEvent(cmd.object.get(0), cmd.object.get(1), args, header);
					eventListener.onEvent(cmdEvent);
				}
			}
		});
		
		cmdEvents.connect();
	}
}
