package com.flipkart.redis.event.data;

import com.flipkart.redis.net.Datatype;

public class KeyTypePair extends AbstractData {
	public String key;
	public Datatype type;

	public KeyTypePair(String key, Datatype type) {
		super();
		this.key = key;
		this.type = type;
	}

	@Override
    public String getKey() {
	    return key;
    }
	public Datatype getDatatype() {
		return type;
	}
}
