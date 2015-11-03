package com.flipkart.redis.replicator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.flipkart.redis.event.generator.CommandEventGenerator;
import com.flipkart.redis.event.generator.KeyValueEventGenerator;
import com.flipkart.redis.event.generator.RDBDataEventGenerator;
import com.flipkart.redis.event.listener.CommandEventListener;
import com.flipkart.redis.event.listener.KeyValueEventListener;
import com.flipkart.redis.net.Connection;
import com.flipkart.redis.net.KeyUpdateObservableMapper;
import com.flipkart.redis.net.KeyUpdateObservableMapper.KeyTypePair;
import com.flipkart.redis.net.Reply;
import com.flipkart.redis.net.rdb.RDBParser.Entry;
import com.flipkart.redis.replicator.state.ReplicatorState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.observables.ConnectableObservable;

public class RedisReplicator {
	
	private static final Logger logger = LoggerFactory.getLogger(RedisReplicator.class);
	
	private int port;
	private String host;
	private String password;
	
	private String masterRunId;
	
	/**
	 * For partial sync, current master_repl_offset can be looked up by running "info replication" in redis-cli 
	 */
	private long initBacklogOffset;
	private Connection connection;
	private int soTimeout;
	
	private CommandEventListener cmdEventListener = null;
	private KeyValueEventListener kvEventListener = null;
	private KeyValueEventListener rdbkvEventListener = null;
	
	/**
	 * executorService for replication task.
	 */
	private ExecutorService singleThreadExecService = null;
	private ScheduledExecutorService scheduledExecService = null;
	
	private List<Future<?>> submittedReplicationTasks = null;
	
	/**
	 * Replicator state to be shared between eventGenerator thread and 
	 * the thread that tells master current replication offset.
	 */
	private ReplicatorState currentState = null;
	
	private boolean fetchFullKeyValueOnUpdate = false;
	
	public RedisReplicator() {
		this("127.0.0.1", 6379);
	}
	
	public RedisReplicator(String host, int port) {
		this.host = host;
		this.port = port;
		this.password = null;
		
		this.masterRunId = "?";
		this.initBacklogOffset = -1;
		
		this.soTimeout = 10000;				// blocking stream operation timeout
		
		scheduledExecService = Executors.newSingleThreadScheduledExecutor();
		singleThreadExecService = Executors.newSingleThreadExecutor();
		
		submittedReplicationTasks = new ArrayList<Future<?>>();
	}
	
	/**
	 * connects to the redis instance and sets up a replication task.
	 * @throws Exception 
	 */
	public void start() throws Exception
	{
		// connect to master
		connection = new Connection(host, port);
		connection.setSoTimeout(soTimeout);
		connection.connect();
		
		if(!connection.isConnected()) {
			throw new RuntimeException("Could not connect");
		}
		
		// authenticate
		if(password != null) {
			connection.authenticate(password);
		}
		
		String masterInfo = connection.getInfo("server");
		int runIdBegin = masterInfo.indexOf("run_id:");
		int runIdEnd = masterInfo.indexOf("\r\n", runIdBegin);
		masterRunId = masterInfo.substring(runIdBegin + 7, runIdEnd);
		
		// request psync
		String syncStatus = connection.requestForPSync(masterRunId, initBacklogOffset + 1);
		String[] statusStrTokens = syncStatus.split(" ");
		
		String syncType = statusStrTokens[0];
		
		if(syncType.startsWith("FULLRESYNC")) {
			masterRunId = statusStrTokens[1];
			initBacklogOffset = Long.parseLong(statusStrTokens[2]);
		}
		
		currentState = new ReplicatorState(masterRunId, initBacklogOffset);
		
		scheduleReplOffsetTellerTask();
		startReplicatorThread(syncType);
		
		logger.info("replication started, status: " + syncStatus);
	}
	
	private void startReplicatorThread(String syncType) {

		// if psync successful
		if(syncType.startsWith("CONTINUE")) {
			startPartialReplicationTask();
		}
		// if partial sync failed. fullresync will be performed.
		else if(syncType.startsWith("FULLRESYNC")) {
			startRDBReplicationTask();
			startPartialReplicationTask();
		}
	}
	
	private void startRDBReplicationTask() {
		final Observable<Entry> rdbObservable = connection.getRdbDump();
		ConnectableObservable<Entry> connectableRDBObservable = rdbObservable.publish();
		
		connectableRDBObservable.subscribe(new RDBDataEventGenerator(rdbkvEventListener, currentState));
		
		submittedReplicationTasks.add(singleThreadExecService.submit(connectInRunnable(connectableRDBObservable)));
	}
	
	private void startPartialReplicationTask() {
		final Observable<Reply<List<String>>> cmdEvents = connection.getCommands();
		
		if(fetchFullKeyValueOnUpdate) {
			
			final KeyUpdateObservableMapper keyUpdateObservableMapper = new KeyUpdateObservableMapper();
			ConnectableObservable<Reply<KeyTypePair>> keyUpdates = cmdEvents.concatMap(e -> keyUpdateObservableMapper.map(e)).publish();
			
			keyUpdates.subscribe(new KeyValueEventGenerator(kvEventListener, currentState, host, port, password, soTimeout));
			
			submittedReplicationTasks.add(singleThreadExecService.submit(connectInRunnable(keyUpdates)));
		}
		else {
			ConnectableObservable<Reply<List<String>>> cmdEventsConnectableObs = cmdEvents.publish();
			cmdEventsConnectableObs.subscribe(new CommandEventGenerator(cmdEventListener, currentState));
			
			submittedReplicationTasks.add(singleThreadExecService.submit(connectInRunnable(cmdEventsConnectableObs)));
		}
	}
	
	/**
	 * schedule a task that tells the master its own replication offset every second
	 */
	private void scheduleReplOffsetTellerTask()
	{
		Runnable offsetTellerTask = new Runnable() {
			public void run() {
				connection.sendReplAck(currentState.getReplicationOffset());
			}
		};
		
		scheduledExecService.scheduleAtFixedRate(offsetTellerTask, 0, 3, TimeUnit.SECONDS);
	}
	
	private <T> Runnable connectInRunnable(ConnectableObservable<T> observable) {
		return new Runnable() {
			public void run() {
				observable.connect();
			}
		};
	}
	
	public void stop() throws InterruptedException {
		scheduledExecService.shutdownNow();
		scheduledExecService.awaitTermination(5, TimeUnit.SECONDS);
		
		//close the connection. it will also stop the worker thread
		connection.close();
	}
	
	public void joinOnReplicationTask() throws ExecutionException, InterruptedException {
		for(Future<?> f : submittedReplicationTasks) {
			f.get();
		}
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
		return soTimeout;
	}

	public void setStreamOpTimeout(int milliseconds) {
		this.soTimeout = milliseconds;
	}
	
	public CommandEventListener getCommandEventListener() {
		return cmdEventListener;
	}

	public void setCommandEventListener(CommandEventListener cmdEventListener) {
		this.cmdEventListener = cmdEventListener;
	}

	public KeyValueEventListener getKeyValueEventListener() {
		return kvEventListener;
	}

	public void setKeyValueEventListener(KeyValueEventListener kvEventListener) {
		this.kvEventListener = kvEventListener;
	}

	public KeyValueEventListener getRdbKeyValueEventListener() {
		return rdbkvEventListener;
	}

	public void setRdbKeyValueEventListener(KeyValueEventListener rdbkvEventListener) {
		this.rdbkvEventListener = rdbkvEventListener;
	}

	public boolean isFetchFullKeyValueOnUpdate() {
		return fetchFullKeyValueOnUpdate;
	}

	public void setFetchFullKeyValueOnUpdate(boolean fetchFullKeyValueOnUpdate) {
		this.fetchFullKeyValueOnUpdate = fetchFullKeyValueOnUpdate;
	}
}
