package com.flipkart.redis.net;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import rx.Observable;
import rx.Subscriber;
import rx.Observable.OnSubscribe;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.EventHeader;
import com.flipkart.redis.event.data.CommandArgsPair;
import com.flipkart.redis.event.data.KeyValuePair;
import com.flipkart.redis.net.Protocol.Command;
import com.flipkart.redis.net.rdb.RDBParser;

public class SlaveConnection extends Connection {

	public static enum ReplicationType {
		FULLRESYNC,
		PARTIAL
	}
	
	private static String defaultMasterID = "?";
	private static long defaultReplicationOffset = -1;

	private String masterRunId = defaultMasterID;
	private long initReplicationOffset = defaultReplicationOffset;
	private AtomicLong currReplicationOffset = new AtomicLong();
	private ReplicationType replicationType;
	
	private long minPingAckDuration = 1000;
	private long lastPingTimestamp = 0;

	public SlaveConnection() {
		super();
	}

	public SlaveConnection(final String host) {
		super(host);
	}

	public SlaveConnection(final String host, final int port) {
		super(host, port);
	}

	protected String sendLocalPort() {
		return sendCommand(Command.REPLCONF, "listening-port", "" + getSocket().getLocalPort()).getStatusCodeReply().object;
	}

	public String requestForPSync() {
		return requestForPSync(defaultReplicationOffset);
	}

	public String requestForPSync(long initReplicationOffset) {

		// populate masterId
		String masterInfo = getInfo("server");
		int runIdBegin = masterInfo.indexOf("run_id:");
		int runIdEnd = masterInfo.indexOf("\r\n", runIdBegin);
		masterRunId = masterInfo.substring(runIdBegin + 7, runIdEnd);

		// notify master server this connections port.
		sendLocalPort();
		
		// request psync
		sendCommand(Command.PSYNC, masterRunId, String.valueOf(initReplicationOffset));

		String status = getStatusCodeReply().object;
		String[] statusTokens = status.split(" ");

		if (statusTokens[0].startsWith("FULLRESYNC")) {
			this.initReplicationOffset = Long.parseLong(statusTokens[2]);
			replicationType = ReplicationType.FULLRESYNC;
		}
		else {
			this.initReplicationOffset = initReplicationOffset - 1;
			replicationType = ReplicationType.PARTIAL;
		}

		return status;
	}

	public void sendReplAck() {
		sendCommand(Command.REPLCONF, "ACK", String.valueOf(currReplicationOffset.get()));
	}

	public Observable<Event<KeyValuePair>> getRdbDump() {

		final RDBParser rdbParser = new RDBParser();
		rdbParser.init(getInputStream());

		Observable<Event<KeyValuePair>> dataEvents = Observable.create(new OnSubscribe<Event<KeyValuePair>>() {

			@Override
			public void call(Subscriber<? super Event<KeyValuePair>> t) {
				try {
					RDBParser.Entry entry = rdbParser.next();

					while (entry != null) {

						// update current replication offset.
						currReplicationOffset.getAndSet(entry.streamOffset);

						Event<KeyValuePair> evt =
						        new Event<KeyValuePair>(new KeyValuePair(entry.key, entry.value,
						                Protocol.rdbInt2DataTypeMapping[entry.type], entry.database), createHeader());
						
						if(!t.isUnsubscribed()) {
							t.onNext(evt);
						}
						entry = rdbParser.next();
					}

					byte[] checksum = new byte[8];
					getInputStream().read(checksum, 0, 8);

					if(!t.isUnsubscribed()) {
						t.onCompleted();
					}
				}
				catch (Exception e) {
					// error has occurred while parsing the stream. stop
					// emitting any more events
					if(!t.isUnsubscribed()) {
						t.onError(e);
					}
				}
			}
		});

		return dataEvents;
	}

	public Observable<Event<CommandArgsPair>> getCommands() {

		Observable<Event<CommandArgsPair>> cmdEvents = Observable.create(new OnSubscribe<Event<CommandArgsPair>>() {
			@Override
			public void call(Subscriber<? super Event<CommandArgsPair>> t) {
				
				// set current offset to init offset because the replication will start from this point
				currReplicationOffset.getAndSet(initReplicationOffset);
				
				try {
					while (true) {
						Reply<List<String>> reply = getMultiBulkReplySafe();

						// advance replication offset
						currReplicationOffset.getAndAdd(reply.bytesRead);

						final Command command = Command.valueOf(reply.object.get(0).toUpperCase());
						
						// if command is PING, send replication ack to master
						if(command.equals(Command.PING)) {
							processPing();
							continue;
						}
						
						// create command event
						List<String> args = reply.object.subList(1, reply.object.size());

						Event<CommandArgsPair> cmdEvent =
						        new Event<CommandArgsPair>(new CommandArgsPair(reply.object.get(0), command
						                .keysUpdated(reply.object), args), createHeader());

						t.onNext(cmdEvent);
					}
				}
				catch (JedisConnectionException e) {
					t.onError(e);
				}
				catch (JedisDataException e) {
					t.onError(e);
				}
			}
		});
		return cmdEvents;
	}
	
	private void processPing() {
		long curTimestamp = System.currentTimeMillis();
		if(curTimestamp - lastPingTimestamp > minPingAckDuration) {
			// process ping
			sendReplAck();
			lastPingTimestamp = curTimestamp;
		}
	}

	private EventHeader createHeader() {
		EventHeader header = new EventHeader(masterRunId, getCurrReplicationOffset());
		return header;
	}

	public ReplicationType getReplicationType() {
		return replicationType;
	}

	public void setReplicationType(ReplicationType replicationType) {
		this.replicationType = replicationType;
	}

	public long getMinPingAckDuration() {
		return minPingAckDuration;
	}

	public void setMinPingAckDuration(long minPingAckDuration) {
		this.minPingAckDuration = minPingAckDuration;
	}

	public String getMasterRunId() {
		return masterRunId;
	}

	public long getInitReplicationOffset() {
		return initReplicationOffset;
	}

	public long getCurrReplicationOffset() {
		return currReplicationOffset.get();
	}
}
