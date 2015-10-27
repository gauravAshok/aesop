package com.flipkart.redis.event;

public class RDBDataEvent extends AbstractEvent {

	String key;	
	Object value;
	Datatype type;
	int database;

	public RDBDataEvent(String key, Object value, Datatype type, int database,
			EventHeader header) {
		super(header);
		this.key = key;
		this.value = value;
		this.type = type;
		this.database = database;
	}

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
