package com.flipkart.redis.event;

import java.util.List;

/**
 * Event for command and its arguments.
 * @author gaurav.ashok
 */
public class CommandEvent extends AbstractEvent {
	
	String command;
	String key;
	List<String> args;
	
	//Datatype type;
	
	public CommandEvent(String command, String key, List<String> args,
			EventHeader header) {
		super(header);
		this.command = command;
		this.key = key;
		this.args = args;
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
}
