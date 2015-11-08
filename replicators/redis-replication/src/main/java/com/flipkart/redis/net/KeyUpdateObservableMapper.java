package com.flipkart.redis.net;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.flipkart.redis.net.Protocol.Command;

import rx.Observable;

public class KeyUpdateObservableMapper {

	public static class KeyTypePair {
		public String key;
		public Datatype type;

		public KeyTypePair(String key, Datatype type) {
			super();
			this.key = key;
			this.type = type;
		}
	}

	boolean txnBegin = false;
	/**
	 * hashmap to maintain keys and its datatype during the transaction
	 */
	Map<String, Datatype> keysModifiedInCurrentTxn = new HashMap<String, Datatype>();
	long bytesReadInCurrentTxn = 0;
	long bytesReadWhileNothingUpdated = 0;

	private void startTxn() {
		txnBegin = true;
		keysModifiedInCurrentTxn.clear();
		bytesReadInCurrentTxn = 0;
	}

	public Observable<Reply<KeyTypePair>> map(Reply<List<String>> cmd) {
		final Command command = Command.valueOf(cmd.object.get(0).toUpperCase());

		if (command == Command.MULTI) {
			startTxn();
			bytesReadInCurrentTxn += cmd.bytesRead;
			return Observable.empty();
		}
		else if (command == Command.EXEC && txnBegin) {
			bytesReadInCurrentTxn += cmd.bytesRead;
			return endTxn();
		}

		if (txnBegin == false) {
			List<String> keysUpdated = command.keysUpdated(cmd.object);

			if (keysUpdated.size() == 0) {
				bytesReadWhileNothingUpdated += cmd.bytesRead;
				return Observable.empty();
			}

			long bytesReadSinceLastUpdate = bytesReadWhileNothingUpdated + cmd.bytesRead;
			bytesReadWhileNothingUpdated = 0;

			List<Reply<KeyTypePair>> updatedKeysWithTypes = new ArrayList<Reply<KeyTypePair>>();
			for (String key : keysUpdated) {
				updatedKeysWithTypes.add(new Reply<KeyTypePair>(new KeyTypePair(key, command.datatype),
				        bytesReadSinceLastUpdate));
			}

			return Observable.from(updatedKeysWithTypes);
		}
		else {
			for (String key : command.keysUpdated(cmd.object)) {
				keysModifiedInCurrentTxn.put(key, command.datatype);
			}
			bytesReadInCurrentTxn += cmd.bytesRead;
		}

		return Observable.empty();
	}

	private Observable<Reply<KeyTypePair>> endTxn() {

		/* end transaction */
		txnBegin = false;

		List<Reply<KeyTypePair>> updatedKeys = new ArrayList<Reply<KeyTypePair>>();
		for (String key : keysModifiedInCurrentTxn.keySet()) {
			updatedKeys.add(new Reply<KeyTypePair>(new KeyTypePair(key, keysModifiedInCurrentTxn.get(key)),
			        bytesReadInCurrentTxn));
		}

		return Observable.from(updatedKeys);
	}
}
