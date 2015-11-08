package com.flipkart.redis.event;

import com.flipkart.redis.net.Datatype;

/**
 * Event to represent a key and associated data as present in redis
 * @author gaurav.ashok
 */
public class KeyValueEvent extends AbstractEvent {

	String key;	
	Object value;
	Datatype type;
	int database;

	public KeyValueEvent(String key, Object value, Datatype type, int database,
			EventHeader header) {
		super(header);
		this.key = key;
		this.value = value;
		this.type = type;
		this.database = database;
	}

	@Override
	public String getKey() {
		return key;
	}
	public Object getValue() {
		return value;
	}
	public Datatype getType() {
		return type;
	}
	public int getDatabase() {
		return database;
	}
}
