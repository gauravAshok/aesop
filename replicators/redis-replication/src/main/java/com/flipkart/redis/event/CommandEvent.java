package com.flipkart.redis.event;

import java.util.List;

public class CommandEvent {
	
	String command;
	List<String> args;
	long masterBacklogOffset;
	
	public CommandEvent(String command, List<String> args, long masterBacklogOffset) {
		this.command = command;
		this.args = args;
		this.masterBacklogOffset = masterBacklogOffset;
	}
	
	public String getCommand() {
		return command;
	}
	public List<String> getArgs() {
		return args;
	}
	public long getMasterBacklogOffset() {
		return masterBacklogOffset;
	}
}
