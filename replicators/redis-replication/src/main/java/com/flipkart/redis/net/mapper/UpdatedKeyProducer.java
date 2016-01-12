package com.flipkart.redis.net.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.EventHeader;
import com.flipkart.redis.event.data.CommandArgsPair;
import com.flipkart.redis.event.data.KeyTypePair;
import com.flipkart.redis.net.Datatype;
import com.flipkart.redis.net.Protocol.Command;

import rx.Observable;

public class UpdatedKeyProducer implements ObservableMapper<Event<CommandArgsPair>, Observable<Event<KeyTypePair>>> {

	boolean txnBegin = false;
	/**
	 * hashmap to maintain keys and its datatype during the transaction
	 */
	Map<String, Datatype> keysModifiedInCurrentTxn = new HashMap<String, Datatype>();
	long bytesReadInCurrentTxn = 0;
	long bytesReadWhileNothingUpdated = 0;
	
	EventHeader eventHeader = null;

	private void startTxn() {
		txnBegin = true;
		keysModifiedInCurrentTxn.clear();
		bytesReadInCurrentTxn = 0;
	}

	public Observable<Event<KeyTypePair>> call(Event<CommandArgsPair> cmd) {
		
		eventHeader = cmd.getHeader();
		
		final Command command = Command.valueOf(cmd.getData().getCommand().toUpperCase());
		
		if (command == Command.MULTI) {
			startTxn();
			return Observable.empty();
		}
		else if (command == Command.EXEC && txnBegin) {
			return endTxn();
		}

		if (txnBegin == false) {
			List<String> keysUpdated = cmd.getData().getKeySet();

			if (keysUpdated == null || keysUpdated.size() == 0) {
				return Observable.empty();
			}

			//TODO  treat it as a mini transaction
			//TODO  all keys in cmd.keySet may not get updated. 
			List<Event<KeyTypePair>> updatedKeysWithTypes = new ArrayList<Event<KeyTypePair>>();
			for (String key : keysUpdated) {
				updatedKeysWithTypes.add(new Event<KeyTypePair>(new KeyTypePair(key, command.datatype),
				        eventHeader));
			}

			return Observable.from(updatedKeysWithTypes);
		}
		else {
			for (String key : cmd.getData().getKeySet()) {
				keysModifiedInCurrentTxn.put(key, command.datatype);
			}
		}

		return Observable.empty();
	}

	private Observable<Event<KeyTypePair>> endTxn() {

		/* end transaction */
		/* will have to look for another way, where new array is not created */
		txnBegin = false;

		List<Event<KeyTypePair>> updatedKeys = new ArrayList<Event<KeyTypePair>>();
		for (String key : keysModifiedInCurrentTxn.keySet()) {
			updatedKeys.add(new Event<KeyTypePair>(new KeyTypePair(key, keysModifiedInCurrentTxn.get(key)),
			        eventHeader));
		}

		return Observable.from(updatedKeys);
	}
}
