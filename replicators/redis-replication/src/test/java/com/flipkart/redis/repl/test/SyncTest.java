package com.flipkart.redis.repl.test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.flipkart.redis.event.BacklogEventListener;
import com.flipkart.redis.event.CommandEvent;
import com.flipkart.redis.event.DataEvent;
import com.flipkart.redis.event.Datatype;
import com.flipkart.redis.net.Connection;
import com.flipkart.redis.net.Protocol.Command;
import com.flipkart.redis.replicator.RedisReplicator;

class TestListener implements BacklogEventListener 
{
	public static int dataEventsCount = 0;
	public static int commEventsCount = 0;
	
	@Override
	public void onEvent(CommandEvent event) {
		System.out.print(event.getMasterBacklogOffset() + " : " + event.getCommand() + " ");
		for(String o : event.getArgs()) {
			System.out.print(o + " ");
		}
		System.out.println();
		
		commEventsCount ++;
	}

	@Override
	public void onEvent(DataEvent event) {
		System.out.print("dump: " + event.getDatabase() + ": " + event.getType() + ": " + event.getKey() + " : " + event.getValue().toString());
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
	
	
    public void testFullSync() throws InterruptedException
    {
		RedisReplicator replicator = new RedisReplicator("127.0.0.1", 6379);
		//replicator.setPassword("password");
		replicator.setEventListener(new TestListener());
		replicator.setStreamOpTimeout(7000);
		
		//try partial sync
		replicator.setMasterId("082b879a177a445304b3074fe3442dbc10f169ba");
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
//		Jedis conn = new Jedis("127.0.0.1", 6379);
//		conn.connect();
		
		//generateData(conn, 0, 100);
		
		RedisReplicator replicator = new RedisReplicator("127.0.0.1", 6379);
		//replicator.setPassword("password");
		replicator.setEventListener(new TestListener());
		replicator.setStreamOpTimeout(7000);
		
//		try partial sync
//		replicator.setMasterId("082b879a177a445304b3074fe3442dbc10f169ba");
//		replicator.setInitBacklogOffset(625);
		
		try {
			replicator.start();
		} catch (Exception e) {
			System.out.println("unexpected things happened. look into it.");
			e.printStackTrace();
		}
		
//		while(true) {
//			Thread.sleep(100);
//			System.out.println("count: " + TestListener.dataEventsCount);
//			if(TestListener.dataEventsCount == 1000000) break;
//		}
		
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
