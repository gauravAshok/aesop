package com.flipkart.redis.event;


public class DataEvent {

	String key;
	Object value;
	int database;
    Datatype type;
	
	public DataEvent(String key, Object value, int database, Datatype type) {
		this.key = key;
		this.value = value;
		this.database = database;
		this.type = type;
	}
    
	public String getKey() {
		return key;
	}
	public Object getValue() {
		return value;
	}
	public int getDatabase() {
		return database;
	}
	public Datatype getType() {
		return type;
	}
}
