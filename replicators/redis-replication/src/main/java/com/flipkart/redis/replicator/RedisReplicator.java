package com.flipkart.redis.replicator;

import com.flipkart.redis.event.BacklogEventListener;
import com.flipkart.redis.net.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.JedisException;

public class RedisReplicator {
	
	private static final Logger logger = LoggerFactory.getLogger(RedisReplicator.class);
	
	private int port;
	private String host;
	private String password;
	
	private String masterRunId;
	private long initBacklogOffset;
	private Connection connection;
	private int streamOpTimeout;
	
	private BacklogEventListener eventListener;
	
	/**
	 * worker thread for replication task.
	 */
	private Thread worker;
	
	public RedisReplicator() {
		this("127.0.0.1", 6379);
	}
	
	public RedisReplicator(String host, int port) {
		this.host = host;
		this.port = port;
		this.password = null;
		
		this.masterRunId = "?";
		this.initBacklogOffset = -1;
		
		this.streamOpTimeout = 10000;				// blocking stream operation timeout
	}
	
	/**
	 * connects to the redis instance and sets up a replication.
	 * @throws Exception 
	 */
	public void start() throws Exception
	{
		// connect to master
		connection = new Connection(host, port);
		connection.setSoTimeout(streamOpTimeout);
		connection.connect();
		
		if(!connection.isConnected()) {
			throw new JedisException("Could not connect");
		}
		
		// authenticate
		if(password != null) {
			connection.authenticate(password);
		}
		
		// request psync
		String syncStatus = connection.requestForPSync(masterRunId, initBacklogOffset + 1);
		
		if(syncStatus.startsWith("CONTINUE")) {
			// psync successful
			SyncTask task = new SyncTask(connection, eventListener, initBacklogOffset);
			worker = new Thread(task);
			worker.start();
		}
		else if(syncStatus.startsWith("FULLRESYNC")) {
			// partial sync failed. fullresync will be performed.
			String[] statusStrTokens = syncStatus.split(" ");
			masterRunId = statusStrTokens[1];
			initBacklogOffset = Long.parseLong(statusStrTokens[2]);
			
			FullSyncTask task = new FullSyncTask(this.connection, eventListener, initBacklogOffset);
			worker = new Thread(task);
			worker.start();
		}
		
		logger.info("replication started, status: " + syncStatus);
	}
	
	public void stop() {
		//close the connection. it will also stop the worker thread
		connection.close();
	}
	
	public void joinOnReplicationTask() throws InterruptedException {
		worker.join();
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getMasterId() {
		return masterRunId;
	}

	public void setMasterId(String masterId) {
		this.masterRunId = masterId;
	}

	public long getInitBacklogOffset() {
		return initBacklogOffset;
	}

	public void setInitBacklogOffset(long initBacklogOffset) {
		this.initBacklogOffset = initBacklogOffset;
	}

	public int getPort() {
		return port;
	}

	public String getHost() {
		return host;
	}

	public long getStreamOpTimeout() {
		return streamOpTimeout;
	}

	public void setStreamOpTimeout(int milliseconds) {
		this.streamOpTimeout = milliseconds;
	}
	
	public BacklogEventListener getEventListener() {
		return eventListener;
	}

	public void setEventListener(BacklogEventListener eventListener) {
		this.eventListener = eventListener;
	}
}
