package com.flipkart.redis.event.generator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.redis.event.CommandEvent;
import com.flipkart.redis.event.listener.BacklogEventListener;
import com.flipkart.redis.net.Reply;
import com.flipkart.redis.replicator.state.ReplicatorState;

public class CommandEventGenerator extends AbstractEventGenerator<Reply<List<String>>> {

	private static final Logger logger = LoggerFactory.getLogger(CommandEventGenerator.class);
	
	public CommandEventGenerator(BacklogEventListener listener, ReplicatorState state) {
		super(listener, state);
	}

	@Override
	public void onCompleted() {
		logger.info("CommandEvents have ended.");
	}

	@Override
	public void onError(Throwable e) {
		eventListener.onException(e);
	}

	@Override
	public void onNext(Reply<List<String>> cmd) {
		
		state.setReplicationOffset(state.getReplicationOffset() + cmd.bytesRead);
		
		if(cmd.object.get(0).equals("PING")) {
			//do nothing
		}
		else {
			List<String> args = cmd.object.subList(2, cmd.object.size());
			
			CommandEvent cmdEvent = new CommandEvent(cmd.object.get(0), cmd.object.get(1), args, this.generateHeader(cmd));
			eventListener.onEvent(cmdEvent);
		}
	}
}
