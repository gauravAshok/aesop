package com.flipkart.redis.repl.test;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.CommandArgsPair;
import com.flipkart.redis.event.data.KeyValuePair;
import com.flipkart.redis.event.listener.CommandEventListener;
import com.flipkart.redis.event.listener.KeyValueEventListener;
import com.flipkart.redis.replicator.RedisReplicator;

class TestcmdListener implements CommandEventListener
{
	public static int commEventsCount = 0;
	
	@Override
	public void onException(Throwable e) {
		System.out.println("some error occurred");
		e.printStackTrace();
	}

	@Override
    public void onEvent(Event<CommandArgsPair> event) {

		System.out.print(Thread.currentThread().getId() + " > ");
		System.out.print(event.getHeader().getMasterBacklogOffset() + " : " + event.getData().getCommand() + " ");
		for(String o : event.getData().getArgs()) {
			System.out.print(o + " ");
		}
		System.out.println();
		
		commEventsCount ++;
	    
    }
}

class TestkvListener implements KeyValueEventListener
{
	public static int dataEventsCount = 0;

	@Override
	public void onException(Throwable e) {
		System.out.println("some error occurred");
		e.printStackTrace();
	}

	@Override
    public void onEvent(Event<KeyValuePair> event) {
		System.out.print(Thread.currentThread().getId() + " > ");
		System.out.print("data: " + event.getData().getDatabase() + ": " + event.getData().getType() + ": " + event.getKey() + " : " + event.getData().getValue().toString() + " Offset: " + event.getHeader().getMasterBacklogOffset());
		System.out.println();
		dataEventsCount++;
    }
}

public class SyncTest {
	
    public void testFullSync() throws InterruptedException, ExecutionException
    {
		RedisReplicator replicator = new RedisReplicator("127.0.0.1", 6379);
		//replicator.setPassword("password");
		//replicator.setCommandEventListener(new TestcmdListener());
		replicator.setKeyValueEventListener(new TestkvListener());
		//replicator.setRdbKeyValueEventListener(new TestkvListener());
		replicator.setSoTimeout(7000);
		replicator.setFetchFullDataOnKeyUpdate(true);
		//try partial sync
		
		//replicator.setInitBacklogOffset(14574);
		
		try {
			replicator.start();
		} catch (Exception e) {
			System.out.println("unexpected things happened. look into it.");
			e.printStackTrace();
		}
		
		//avoid termination of the program
		while(replicator.isRunning()) {
			Thread.sleep(1000);
		}
    }
}
