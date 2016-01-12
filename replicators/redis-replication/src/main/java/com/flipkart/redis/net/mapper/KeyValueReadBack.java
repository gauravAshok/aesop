package com.flipkart.redis.net.mapper;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.KeyTypePair;
import com.flipkart.redis.event.data.KeyValuePair;
import com.flipkart.redis.net.Connection;
import com.flipkart.redis.net.Datatype;
import com.flipkart.redis.net.Protocol.Command;

public class KeyValueReadBack implements ObservableMapper<Event<KeyTypePair>, Event<KeyValuePair>> {

	private static final Logger logger = LoggerFactory.getLogger(KeyValueReadBack.class);

	private Connection connection;

	public KeyValueReadBack(Connection conn) {
		this.connection = conn;
	}

	@Override
	public Event<KeyValuePair> call(Event<KeyTypePair> item) {

		logger.debug("{} updated", item.getKey());

		String key = item.getKey();
		Datatype type = item.getData().getDatatype();
		Object value = null;

		switch (type) {
			case HASH :
				value = getHashMap(key);
				break;
			case LIST :
				value = getList(key);
				break;
			case SET :
				value = getSet(key);
				break;
			case STRING :
				value = getString(key);
				break;
			case ZSET :
				value = getZSet(key);
				break;
		}

		Event<KeyValuePair> kvPair =
		        new Event<KeyValuePair>(new KeyValuePair(key, value, item.getData().type, -1), item.getHeader());

		return kvPair;
	}

	private String getString(String key) {
		connection.sendCommand(Command.GET, key);
		return connection.readString();
	}

	private List<String> getList(String key) {
		connection.sendCommand(Command.LRANGE, key, "0", "-1");
		return connection.readList();
	}

	private List<String> getSet(String key) {
		connection.sendCommand(Command.SMEMBERS, key);
		return connection.readSet();
	}

	private List<String> getZSet(String key) {
		connection.sendCommand(Command.ZRANGE, key, "0", "-1");
		return connection.readSet();
	}

	private Map<String, String> getHashMap(String key) {
		connection.sendCommand(Command.HGETALL, key);
		return connection.readHashMap();
	}
}
