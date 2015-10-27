package com.flipkart.redis.event;

import java.util.List;

/**
 * Event for command and its arguments. Optionally it may also contain the whole data for key.
 * @author gaurav.ashok
 */
public class CommandEvent extends AbstractEvent {
	
	String command;
	String key;
	List<String> args;
	
	Object value;
	Datatype type;
	
	public CommandEvent(String command, String key, List<String> args, Object value, Datatype type,
			EventHeader header) {
		super(header);
		this.command = command;
		this.key = key;
		this.args = args;
		this.value = value;
		this.type = type;
	}
	
	public CommandEvent(String command, String key, List<String> args,
			EventHeader header) {
		this(command, key, args, null, null, header);
	}
	
	public String getCommand() {
		return command;
	}
	public String getKey() {
		return key;
	}
	public List<String> getArgs() {
		return args;
	}
	public Object getValue() {
		return value;
	}
	public Datatype getType() {
		return type;
	}
	public boolean isDataPresent() {
		return value != null;
	}
}
