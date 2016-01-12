package com.flipkart.redis.replicator;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.util.JedisURIHelper;
import rx.Observable;
import rx.observables.ConnectableObservable;
import rx.schedulers.Schedulers;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.*;
import com.flipkart.redis.event.listener.AbstractEventListener;
import com.flipkart.redis.event.relay.EventRelay;
import com.flipkart.redis.exception.ReplicatorRuntimeException;
import com.flipkart.redis.net.*;
import com.flipkart.redis.net.SlaveConnection.ReplicationType;
import com.flipkart.redis.net.mapper.*;

public class RedisReplicator {

	private static final Logger logger = LoggerFactory.getLogger(RedisReplicator.class);

	private int port;
	private String host;
	private String password = null;
	private int soTimeout = 10000; // 10 sec

	private String masterRunId = "?";

	/**
	 * For partial sync, current master_repl_offset can be looked up by running "info replication" in redis-cli
	 */
	private long initBacklogOffset = -1;

	/**
	 * Slave connection to redis master
	 */
	private SlaveConnection slaveConnection = null;

	/**
	 * Read connection, in case we have to read back values
	 */
	private Connection readConnection = null;

	/**
	 * Event Listeners
	 */
	private AbstractEventListener<Event<CommandArgsPair>> cmdEventListener = null;
	private AbstractEventListener<Event<KeyValuePair>> kvEventListener = null;
	private AbstractEventListener<Event<KeyValuePair>> rdbkvEventListener = null;

	/**
	 * executorService for replication task.
	 */
	private ExecutorService singleThreadExecService = Executors.newSingleThreadExecutor();

	/**
	 * flag to enable fetching full value for the keys using another connection.
	 */
	private boolean fetchFullKeyValueOnUpdate = false;

	private AtomicBoolean isRunning = new AtomicBoolean(false);

	public RedisReplicator() {
		this("127.0.0.1", 6379);
	}

	public RedisReplicator(String host, int port) {
		this.host = host;
		this.port = port;
		this.password = null;
	}

	public RedisReplicator(URI uri) {
		if (!JedisURIHelper.isValid(uri)) {
			throw new InvalidURIException(String.format("Cannot open Redis connection due to invalid URI. %s",
			        uri.toString()));
		}

		this.host = uri.getHost();
		this.port = uri.getPort();
		this.password = JedisURIHelper.getPassword(uri);
	}

	/**
	 * connects to the redis instance and sets up a replication task.
	 * @throws Exception
	 */
	public void start() throws Exception {

		try {
			// connect to master as a slave
			slaveConnection =
			        ConnectionFactory.getInstance().createAsSlave(host, port, password, soTimeout, initBacklogOffset + 1);
	
			if (slaveConnection == null || !slaveConnection.isConnected()) {
				throw new ReplicatorRuntimeException("Could not connect to host @ " + host);
			}
	
			// get masterID
			masterRunId = slaveConnection.getMasterRunId();
	
			if (slaveConnection.getReplicationType() == ReplicationType.FULLRESYNC && rdbkvEventListener == null) {
				logger.warn("No listener registered for RDB dump");
			}
	
			if (slaveConnection.getReplicationType() == ReplicationType.PARTIAL) {
				if (fetchFullKeyValueOnUpdate && kvEventListener == null) {
					throw new ReplicatorRuntimeException("No key-value listener registered");
				}
				else if (!fetchFullKeyValueOnUpdate && cmdEventListener == null) {
					throw new ReplicatorRuntimeException("No command listener registered");
				}
			}
	
			startReplicatorTask();
	
			isRunning.set(true);
			
			logger.info("replication started, status: " + slaveConnection.getReplicationType() + ", offset: "
			        + slaveConnection.getInitReplicationOffset());
		}
		catch(Exception e) {
			// ensure connection is closed
			closeConnections();
			
			// propagate the error
			throw e;
		}
	}

	private void startReplicatorTask() {

		// If partial
		if (slaveConnection.getReplicationType() == ReplicationType.PARTIAL) {
			startPartialReplicationTask();
		}
		// if partial sync failed. fullresync will be performed.
		else {
			startRDBReplicationTask();
			startPartialReplicationTask();
		}
	}

	private void startRDBReplicationTask() {
		
		ConnectableObservable<Event<KeyValuePair>> connectableRDBObservable =
		        slaveConnection.getRdbDump().subscribeOn(Schedulers.from(singleThreadExecService)).publish();
		
		if(rdbkvEventListener != null) {
			connectableRDBObservable.subscribe(new EventRelay<Event<KeyValuePair>>(rdbkvEventListener));
		}
		
		connectableRDBObservable.connect();
	}

	private void startPartialReplicationTask() {
		final Observable<Event<CommandArgsPair>> cmdEvents = slaveConnection.getCommands();

		if (fetchFullKeyValueOnUpdate) {

			if (kvEventListener != null) {
				// create another connection to read back values.
				readConnection = ConnectionFactory.getInstance().create(host, port, password, soTimeout);
				if (!readConnection.isConnected()) {
					throw new ReplicatorRuntimeException("could not connect to host@" + host);
				}

				ConnectableObservable<Event<KeyValuePair>> keyValueObs =
				        cmdEvents.concatMap(new UpdatedKeyProducer()).map(new KeyValueReadBack(readConnection))
				                .subscribeOn(Schedulers.from(singleThreadExecService)).publish();

				// subscribe
				keyValueObs.subscribe(new EventRelay<Event<KeyValuePair>>(kvEventListener));

				keyValueObs.connect();
			}
		}
		else {
			if (cmdEventListener != null) {
				ConnectableObservable<Event<CommandArgsPair>> cmdEventsObs =
				        cmdEvents.subscribeOn(Schedulers.from(singleThreadExecService)).publish();

				cmdEventsObs.subscribe(new EventRelay<Event<CommandArgsPair>>(cmdEventListener));
				cmdEventsObs.connect();
			}
		}
	}
	
	private void closeConnections() {
		if (slaveConnection != null) {
			slaveConnection.close();
		}

		if (readConnection != null) {
			readConnection.close();
		}
	}

	/**
	 * stops the replicator. timeout in seconds
	 * @param timeout
	 * @throws InterruptedException
	 */
	public void stop(int timeout) throws InterruptedException {

		singleThreadExecService.shutdownNow();
		singleThreadExecService.awaitTermination(timeout, TimeUnit.MILLISECONDS);

		// close the connection. it will also stop the worker thread
		closeConnections();
	}

	public boolean isRunning() {
		return isRunning.get();
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

	public long getSoTimeout() {
		return soTimeout;
	}

	public void setSoTimeout(int milliseconds) {
		this.soTimeout = milliseconds;
	}

	public AbstractEventListener<Event<CommandArgsPair>> getCommandEventListener() {
		return cmdEventListener;
	}

	public void setCommandEventListener(AbstractEventListener<Event<CommandArgsPair>> cmdEventListener) {
		this.cmdEventListener = cmdEventListener;
	}

	public AbstractEventListener<Event<KeyValuePair>> getKeyValueEventListener() {
		return kvEventListener;
	}

	public void setKeyValueEventListener(AbstractEventListener<Event<KeyValuePair>> kvEventListener) {
		this.kvEventListener = kvEventListener;
	}

	public AbstractEventListener<Event<KeyValuePair>> getRdbKeyValueEventListener() {
		return rdbkvEventListener;
	}

	public void setRdbKeyValueEventListener(AbstractEventListener<Event<KeyValuePair>> rdbkvEventListener) {
		this.rdbkvEventListener = rdbkvEventListener;
	}

	public boolean isFetchFullKeyValueOnUpdate() {
		return fetchFullKeyValueOnUpdate;
	}

	public void setFetchFullKeyValueOnUpdate(boolean fetchFullKeyValueOnUpdate) {
		this.fetchFullKeyValueOnUpdate = fetchFullKeyValueOnUpdate;
	}
}
