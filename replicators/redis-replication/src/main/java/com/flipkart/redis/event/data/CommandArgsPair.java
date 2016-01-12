package com.flipkart.redis.event.data;

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

public class CommandArgsPair extends AbstractData {
	String command;
	List<String> args;
	
	List<String> keySet;

	public CommandArgsPair(String command, List<String> keySet, List<String> args) {
		this.command = command;
		this.args = args;
		this.keySet = keySet;
	}
	
	@Override
    public String getKey() {
		if(keySet == null || keySet.isEmpty()) {
	    	return null;
	    }
	    return keySet.get(0);
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
}
