package com.flipkart.redis.event;

import java.util.List;

/**
 * Event for command and its arguments.
 * command: first word of the 'REDIS COMMAND'
 * args:    remaining string post the first word are arguments to that command
 * keySet:  list containing of all the keys that are used in the command
 * Example:
 * 				SET        someKey   value1
 * 		     |command|     |     args     |
 * 						   |keySet|
 * 				
 * @author gaurav.ashok
 */
public class CommandEvent extends AbstractEvent {
	
	String command;
	List<String> args;
	
	List<String> keySet;
	
	public CommandEvent(String command, String key, List<String> args,
			EventHeader header) {
		super(header);
		this.command = command;
		this.args = args;
		keySet = null;
	}
	
	void setKeySet(List<String> keySet) {
		this.keySet = keySet;
	}
	
	public String getCommand() {
		return command;
	}
	public List<String> getArgs() {
		return args;
	}
	public List<String> getKeySet() {
		return keySet;
	}
	
	@Override
	public String getKey() {
	    if(keySet == null || keySet.isEmpty()) {
	    	return null;
	    }
	    return keySet.get(0);
	}
}
