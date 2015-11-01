package com.flipkart.redis.event.generator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.redis.event.DataEvent;
import com.flipkart.redis.event.listener.BacklogEventListener;
import com.flipkart.redis.net.Datatype;
import com.flipkart.redis.net.KeyUpdateObservableMapper.KeyTypePair;
import com.flipkart.redis.net.Reply;
import com.flipkart.redis.replicator.state.ReplicatorState;

import redis.clients.jedis.Jedis;

public class KeyValueEventGenerator extends AbstractEventGenerator<Reply<KeyTypePair>>{

	private static final Logger logger = LoggerFactory.getLogger(KeyValueEventGenerator.class);
	private Jedis redisConn = null;
	
	public KeyValueEventGenerator(BacklogEventListener listener, ReplicatorState state, String host, int port, int timeout) {
		super(listener, state);
		redisConn = new Jedis(host, port, timeout);
		redisConn.connect();
		if(!redisConn.isConnected()) {
			throw new RuntimeException("could not establish connection to redis @ " + host);
		}
	}

	@Override
	public void onCompleted() {
		logger.info("KeyUpdates have ended.");	
	}

	@Override
	public void onError(Throwable e) {
		eventListener.onException(e);
	}

	@Override
	public void onNext(Reply<KeyTypePair> keyTypePair) {
		
		logger.debug(keyTypePair.object.key + " updated " + keyTypePair.bytesRead);
		
		state.setReplicationOffset(state.getReplicationOffset() + keyTypePair.bytesRead);
		
		DataEvent event = null;
		String key = keyTypePair.object.key;
		Datatype type = keyTypePair.object.type;
		
		switch(type) {
		case HASH:
			event = new DataEvent(key, getHashMap(key), type, -1, generateHeader(keyTypePair));
			break;
		case LIST:
			event = new DataEvent(key, getList(key), type, -1, generateHeader(keyTypePair));
			break;
		case SET:
			event = new DataEvent(key, getSet(key), type, -1, generateHeader(keyTypePair));
			break;
		case STRING:
			event = new DataEvent(key, getString(key), type, -1, generateHeader(keyTypePair));
			break;
		case ZSET:
			event = new DataEvent(key, getZSet(key), type, -1, generateHeader(keyTypePair));
			break;
		}
		
		eventListener.onEvent(event);
	}
	
	private String getString(String key) {
		return redisConn.get(key);
	}
	
	private List<String> getList(String key) {
		return redisConn.lrange(key, 0, -1);
	}
	
	private Set<String> getSet(String key) {
		return redisConn.smembers(key);
	}
	
	private Set<String> getZSet(String key) {
		return redisConn.zrange(key, 0, -1);
	}
	
	private Map<String, String> getHashMap(String key) {
		return redisConn.hgetAll(key);
	}
}
