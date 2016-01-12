package com.flipkart.redis.event.data;

import com.flipkart.redis.net.Datatype;

public class KeyValuePair extends AbstractData {

	String key;	
	Object value;
	Datatype type;
	int database;
	
	public KeyValuePair(String key, Object value, Datatype type, int database) {
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
