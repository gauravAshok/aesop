package com.flipkart.redis.repl.test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.flipkart.redis.event.CommandEvent;
import com.flipkart.redis.event.KeyValueEvent;
import com.flipkart.redis.event.listener.CommandEventListener;
import com.flipkart.redis.event.listener.KeyValueEventListener;
import com.flipkart.redis.net.Datatype;
import com.flipkart.redis.replicator.RedisReplicator;

class TestcmdListener implements CommandEventListener
{
	public static int commEventsCount = 0;
	
	@Override
	public void onEvent(CommandEvent event) {
		System.out.print(Thread.currentThread().getId() + " > ");
		System.out.print(event.getHeader().getMasterBacklogOffset() + " : " + event.getCommand() + " " + event.getKey() + " ");
		for(String o : event.getArgs()) {
			System.out.print(o + " ");
		}
		System.out.println();
		
		commEventsCount ++;
	}
	
	@Override
	public void onException(Throwable e) {
		System.out.println("some error occurred");
		e.printStackTrace();
	}
}

class TestkvListener implements KeyValueEventListener
{
	public static int dataEventsCount = 0;

	@Override
	public void onEvent(KeyValueEvent event) {
		System.out.print(Thread.currentThread().getId() + " > ");
		System.out.print("dump: " + event.getDatabase() + ": " + event.getType() + ": " + event.getKey() + " : " + event.getValue().toString() + " Offset: " + event.getHeader().getMasterBacklogOffset());
		if(event.getType() == Datatype.STRING) {
			byte[] val = ((String)event.getValue()).getBytes();
			
			System.out.print("  binary: ");
			for (byte theByte : val)
			{
			  System.out.print(String.format("%8s", Integer.toBinaryString(theByte & 0xFF)).replace(' ', '0') + " ");
			}
		}
		System.out.println();
		dataEventsCount++;
	}

	@Override
	public void onException(Throwable e) {
		System.out.println("some error occurred");
		e.printStackTrace();
	}
}

public class SyncTest {
	
	
    public void testFullSync() throws InterruptedException, ExecutionException
    {
		RedisReplicator replicator = new RedisReplicator("127.0.0.1", 6379);
		//replicator.setPassword("password");
		replicator.setCommandEventListener(new TestcmdListener());
		replicator.setKeyValueEventListener(new TestkvListener());
		replicator.setStreamOpTimeout(7000);
		
		//try partial sync
		
		replicator.setInitBacklogOffset(625);
		
		try {
			replicator.start();
		} catch (Exception e) {
			System.out.println("unexpected things happened. look into it.");
			e.printStackTrace();
		}
		
		//avoid termination of the program
		replicator.joinOnReplicationTask();
    }
	
	@Test
    public void testFullSyncWithHugeDataAndRDBProcessing() throws Exception
    {
		System.out.println(Thread.currentThread().getId());
		
		RedisReplicator replicator = new RedisReplicator("127.0.0.1", 6379);
		//replicator.setPassword("password");
		replicator.setCommandEventListener(new TestcmdListener());
		TestkvListener kvl = new TestkvListener();
		replicator.setKeyValueEventListener(kvl);
		replicator.setRdbKeyValueEventListener(kvl);
		replicator.setStreamOpTimeout(7000);

		//replicator.setFetchFullKeyValueOnUpdate(true);
		
//		try partial sync
//		replicator.setInitBacklogOffset(625);
		
		try {
			replicator.start();
		} catch (Exception e) {
			System.out.println("unexpected things happened. look into it.");
			e.printStackTrace();
		}
		
		replicator.joinOnReplicationTask();
    }
	
	private void generateData(Jedis conn, int start, int stop) throws Exception
	{
		List<String> uids = new ArrayList<String>();
		for(int i = start; i < stop; ++i)
		{
			uids.add(UUID.randomUUID().toString());
			conn.set(String.valueOf(i), uids.get(uids.size() - 1));
		}
		
		//verify
		for(int i = start; i < stop; ++i)
		{
			if (!conn.get(String.valueOf(i)).equals(uids.get(i-start)))
				throw new Exception("something is wrong");
		}
	}
}
