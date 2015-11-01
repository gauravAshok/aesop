package com.flipkart.redis.net;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;
import redis.clients.util.SafeEncoder;

public final class Protocol {

	private static final String ASK_RESPONSE = "ASK";
	private static final String MOVED_RESPONSE = "MOVED";
	private static final String CLUSTERDOWN_RESPONSE = "CLUSTERDOWN";
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 6379;
	public static final int DEFAULT_SENTINEL_PORT = 26379;
	public static final int DEFAULT_TIMEOUT = 2000;
	public static final int DEFAULT_DATABASE = 0;

	public static final String CHARSET = "UTF-8";

	public static final byte DOLLAR_BYTE = '$';
	public static final byte ASTERISK_BYTE = '*';
	public static final byte PLUS_BYTE = '+';
	public static final byte MINUS_BYTE = '-';
	public static final byte COLON_BYTE = ':';

	public static final String SENTINEL_MASTERS = "masters";
	public static final String SENTINEL_GET_MASTER_ADDR_BY_NAME = "get-master-addr-by-name";
	public static final String SENTINEL_RESET = "reset";
	public static final String SENTINEL_SLAVES = "slaves";
	public static final String SENTINEL_FAILOVER = "failover";
	public static final String SENTINEL_MONITOR = "monitor";
	public static final String SENTINEL_REMOVE = "remove";
	public static final String SENTINEL_SET = "set";

	public static final String CLUSTER_NODES = "nodes";
	public static final String CLUSTER_MEET = "meet";
	public static final String CLUSTER_RESET = "reset";
	public static final String CLUSTER_ADDSLOTS = "addslots";
	public static final String CLUSTER_DELSLOTS = "delslots";
	public static final String CLUSTER_INFO = "info";
	public static final String CLUSTER_GETKEYSINSLOT = "getkeysinslot";
	public static final String CLUSTER_SETSLOT = "setslot";
	public static final String CLUSTER_SETSLOT_NODE = "node";
	public static final String CLUSTER_SETSLOT_MIGRATING = "migrating";
	public static final String CLUSTER_SETSLOT_IMPORTING = "importing";
	public static final String CLUSTER_SETSLOT_STABLE = "stable";
	public static final String CLUSTER_FORGET = "forget";
	public static final String CLUSTER_FLUSHSLOT = "flushslots";
	public static final String CLUSTER_KEYSLOT = "keyslot";
	public static final String CLUSTER_COUNTKEYINSLOT = "countkeysinslot";
	public static final String CLUSTER_SAVECONFIG = "saveconfig";
	public static final String CLUSTER_REPLICATE = "replicate";
	public static final String CLUSTER_SLAVES = "slaves";
	public static final String CLUSTER_FAILOVER = "failover";
	public static final String CLUSTER_SLOTS = "slots";
	public static final String PUBSUB_CHANNELS = "channels";
	public static final String PUBSUB_NUMSUB = "numsub";
	public static final String PUBSUB_NUM_PAT = "numpat";

	public static final byte[] BYTES_TRUE = toByteArray(1);
	public static final byte[] BYTES_FALSE = toByteArray(0);

	private Protocol() {
		// this prevent the class from instantiation
	}

	public static void sendCommand(final RedisOutputStream os,
			final Command command, final byte[]... args) {
		sendCommand(os, command.raw, args);
	}

	private static void sendCommand(final RedisOutputStream os,
			final byte[] command, final byte[]... args) {
		try {
			os.write(ASTERISK_BYTE);
			os.writeIntCrLf(args.length + 1);
			os.write(DOLLAR_BYTE);
			os.writeIntCrLf(command.length);
			os.write(command);
			os.writeCrLf();

			for (final byte[] arg : args) {
				os.write(DOLLAR_BYTE);
				os.writeIntCrLf(arg.length);
				os.write(arg);
				os.writeCrLf();
			}
		} catch (IOException e) {
			throw new JedisConnectionException(e);
		}
	}

	private static void processError(final RedisInputStream is) {
		String message = is.readLine();
		// TODO: I'm not sure if this is the best way to do this.
		// Maybe Read only first 5 bytes instead?
		if (message.startsWith(MOVED_RESPONSE)) {
			String[] movedInfo = parseTargetHostAndSlot(message);
			throw new JedisMovedDataException(message, new HostAndPort(
					movedInfo[1], Integer.valueOf(movedInfo[2])),
					Integer.valueOf(movedInfo[0]));
		} else if (message.startsWith(ASK_RESPONSE)) {
			String[] askInfo = parseTargetHostAndSlot(message);
			throw new JedisAskDataException(message, new HostAndPort(
					askInfo[1], Integer.valueOf(askInfo[2])),
					Integer.valueOf(askInfo[0]));
		} else if (message.startsWith(CLUSTERDOWN_RESPONSE)) {
			throw new JedisClusterException(message);
		}
		throw new JedisDataException(message);
	}

	public static String readErrorLineIfPossible(RedisInputStream is) {
		final byte b = is.readByte();
		// if buffer contains other type of response, just ignore.
		if (b != MINUS_BYTE) {
			return null;
		}
		return is.readLine();
	}

	private static String[] parseTargetHostAndSlot(
			String clusterRedirectResponse) {
		String[] response = new String[3];
		String[] messageInfo = clusterRedirectResponse.split(" ");
		String[] targetHostAndPort = messageInfo[2].split(":");
		response[0] = messageInfo[1];
		response[1] = targetHostAndPort[0];
		response[2] = targetHostAndPort[1];
		return response;
	}
	
	private static Reply<?> process(final RedisInputStream is, byte b) {
		Reply<?> reply;
		if (b == PLUS_BYTE) {
			reply = processStatusCodeReply(is);
		} else if (b == DOLLAR_BYTE) {
			reply = processBulkReply(is);
		} else if (b == ASTERISK_BYTE) {
			reply = processMultiBulkReply(is);
		} else if (b == COLON_BYTE) {
			reply = processInteger(is);
		} else if (b == MINUS_BYTE) {
			processError(is);
			return null;
		} else {
			throw new JedisConnectionException("Unknown reply: " + (char) b);
		}
		return reply;
	}

	private static Reply<?> process(final RedisInputStream is) {
		final byte b = is.readByte();
		Reply<?> reply = process(is, b);
		reply.bytesRead += 1;
		return reply;
	}
	
	/* method that reads input stream and neglects all data untill a Reply begins with byte b*/
	private static Reply processReply(final RedisInputStream is, byte b) {
		
		int bytesRead = 1;
		byte nextByte = is.readByte();
		while(nextByte != b) {
			if(nextByte == '\r') {
				is.readByte();		// read '\n'
				bytesRead += 1;
			}
			else if(nextByte != '\n') {
				byte[] line = is.readLineBytes();
				bytesRead += line.length;
				bytesRead += 2;
			}
			nextByte = is.readByte();
			bytesRead += 1;
		}
		
		Reply<?> reply = process(is, b);
		reply.bytesRead += bytesRead;
		return reply;
	}

	private static Reply<byte[]> processStatusCodeReply(
			final RedisInputStream is) {
		byte[] statusReply = is.readLineBytes();
		return new Reply<byte[]>(statusReply, statusReply.length + 2);
	}

	private static Reply<byte[]> processBulkReply(final RedisInputStream is) {

		final Reply<byte[]> read = processBulkBinaryReply(is);

		// read 2 more bytes for the command delimiter
		is.readByte();
		is.readByte();

		read.bytesRead = read.bytesRead + 2;

		return read;
	}

	private static Reply<byte[]> processBulkBinaryReply(
			final RedisInputStream is) {

		final int len = is.readIntCrLf();
		if (len == -1) {
			return null;
		}

		final byte[] read = new byte[len];
		int offset = 0;
		while (offset < len) {
			final int size = is.read(read, offset, (len - offset));
			if (size == -1)
				throw new JedisConnectionException(
						"It seems like server has closed the connection.");
			offset += size;
		}

		return new Reply<byte[]>(read, read.length
				+ String.valueOf(len).length() + 2);
	}

	private static Reply<Long> processInteger(final RedisInputStream is) {
		Long num = is.readLongCrLf();
		return new Reply<Long>(num, num.toString().length() + 2);
	}

	private static Reply<List<Object>> processMultiBulkReply(
			final RedisInputStream is) {
		final int num = is.readIntCrLf();
		if (num == -1) {
			return null;
		}

		int bytesRead = String.valueOf(num).length() + 2;

		final List<Object> ret = new ArrayList<Object>(num);
		for (int i = 0; i < num; i++) {
			try {
				Reply read = process(is);
				ret.add(read.object);
				bytesRead += read.bytesRead;
			} catch (JedisDataException e) {
				ret.add(e);
			}
		}

		return new Reply<List<Object>>(ret, bytesRead);
	}

	public static Reply read(final RedisInputStream is) {

		return process(is);
	}
	
	public static Reply read(final RedisInputStream is, byte b) {
		return processReply(is, b);
	}

	public static final byte[] toByteArray(final boolean value) {
		return value ? BYTES_TRUE : BYTES_FALSE;
	}

	public static final byte[] toByteArray(final int value) {
		return SafeEncoder.encode(String.valueOf(value));
	}

	public static final byte[] toByteArray(final long value) {
		return SafeEncoder.encode(String.valueOf(value));
	}

	public static final byte[] toByteArray(final double value) {
		return SafeEncoder.encode(String.valueOf(value));
	}

	public static enum Command {
		PING, 
		SET(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		GET, QUIT, EXISTS, DEL, TYPE, FLUSHDB, KEYS, RANDOMKEY, RENAME, RENAMENX, RENAMEX, DBSIZE, EXPIRE, EXPIREAT, TTL, SELECT, MOVE, FLUSHALL, 
		GETSET(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		MGET(Datatype.STRING), 
		SETNX(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SETEX(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		MSET(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				List<String> updatedKeys = new ArrayList<String>();
				for(int i = 1; i < c.size(); i += 2) {
					updatedKeys.add(c.get(i));
				}
				return updatedKeys;
			}
		}, 
		MSETNX(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				List<String> updatedKeys = new ArrayList<String>();
				for(int i = 1; i < c.size(); i += 2) {
					updatedKeys.add(c.get(i));
				}
				return updatedKeys;
			}
		}, 
		DECRBY(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		DECR(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		INCRBY(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		INCR(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		APPEND(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SUBSTR(Datatype.STRING), 
		HSET(Datatype.HASH) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, HGET(Datatype.HASH),
		HSETNX(Datatype.HASH) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, HMSET(Datatype.HASH) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		HMGET(Datatype.HASH), 
		HINCRBY(Datatype.HASH) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		HEXISTS, 
		HDEL(Datatype.HASH) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		HLEN(Datatype.HASH), HKEYS(Datatype.HASH), HVALS(Datatype.HASH), HGETALL(Datatype.HASH), 
		RPUSH(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		LPUSH(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		LLEN(Datatype.LIST), LRANGE(Datatype.LIST), 
		LTRIM(Datatype.LIST) { //
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		LINDEX(Datatype.LIST), 
		LSET(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		LREM(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		LPOP(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		RPOP(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		RPOPLPUSH(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1, 2);
			}
		}, 
		SADD(Datatype.SET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SMEMBERS(Datatype.SET), 
		SREM(Datatype.SET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SPOP(Datatype.SET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SMOVE(Datatype.SET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1, 2);
			}
		}, 
		SCARD(Datatype.SET), SISMEMBER(Datatype.SET), SINTER(Datatype.SET), 
		SINTERSTORE(Datatype.SET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SUNION(Datatype.SET), 
		SUNIONSTORE(Datatype.SET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SDIFF(Datatype.SET), 
		SDIFFSTORE(Datatype.SET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SRANDMEMBER(Datatype.SET), 
		ZADD(Datatype.ZSET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		ZRANGE(Datatype.ZSET), 
		ZREM(Datatype.ZSET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		ZINCRBY(Datatype.ZSET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		ZRANK(Datatype.ZSET), ZREVRANK(Datatype.ZSET), ZREVRANGE(Datatype.ZSET), ZCARD(Datatype.ZSET), ZSCORE(Datatype.ZSET), MULTI, DISCARD, EXEC, WATCH, UNWATCH, SORT, 
		BLPOP(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		BRPOP(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		AUTH, SUBSCRIBE, PUBLISH, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB, ZCOUNT(Datatype.ZSET), ZRANGEBYSCORE(Datatype.ZSET), ZREVRANGEBYSCORE(Datatype.ZSET), 
		ZREMRANGEBYRANK(Datatype.ZSET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		ZREMRANGEBYSCORE(Datatype.ZSET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		ZUNIONSTORE(Datatype.ZSET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		ZINTERSTORE(Datatype.ZSET) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		ZLEXCOUNT(Datatype.ZSET), ZRANGEBYLEX(Datatype.ZSET), ZREVRANGEBYLEX(Datatype.ZSET), 
		ZREMRANGEBYLEX(Datatype.ZSET){
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SAVE, BGSAVE, BGREWRITEAOF, LASTSAVE, SHUTDOWN, INFO, MONITOR, SLAVEOF, CONFIG, STRLEN(Datatype.STRING), SYNC, 
		LPUSHX(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		PERSIST, 
		RPUSHX(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		ECHO, 
		LINSERT(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		DEBUG, 
		BRPOPLPUSH(Datatype.LIST) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		SETBIT(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		GETBIT(Datatype.STRING), BITPOS(Datatype.STRING), 
		SETRANGE(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 1);
			}
		}, 
		GETRANGE(Datatype.STRING), EVAL, EVALSHA, SCRIPT, SLOWLOG, OBJECT, BITCOUNT(Datatype.STRING), 
		BITOP(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 2);
			}
		}, 
		SENTINEL, DUMP, RESTORE, PEXPIRE, PEXPIREAT, PTTL, 
		INCRBYFLOAT(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 2);
			}
		}, 
		PSETEX(Datatype.STRING) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 2);
			}
		}, 
		CLIENT, TIME, MIGRATE, 
		HINCRBYFLOAT(Datatype.HASH) {
			@Override
			public List<String> keysUpdated(List<String> c) {
				return argAtIndex(c, 2);
			}
		}, 
		SCAN, HSCAN, SSCAN, ZSCAN, WAIT, CLUSTER, ASKING, PFADD, PFCOUNT, PFMERGE, PSYNC, REPLCONF;

		public final byte[] raw;
		
		// datatype of the value on which this command acts on
		public final Datatype datatype; 
		
		Command() {
			this(null);
		}

		Command(Datatype datatype) {
			this.raw = SafeEncoder.encode(this.name());
			this.datatype = datatype;
		}
		
		public List<String> keysUpdated(List<String> c) {
			return Collections.emptyList();
		}
		
		private static List<String> argAtIndex(List<String> c, int... index) {
			if (index.length == 1) {
				return Collections.singletonList(c.get(index[0]));
			}
		
			List<String> updatedKeys = new ArrayList<String>();
			for(int i = 0; i < index.length; ++i) {
				updatedKeys.add(c.get(index[i]));
			}
			return updatedKeys;
		}
	}

	public static enum Keyword {
		AGGREGATE, ALPHA, ASC, BY, DESC, GET, LIMIT, MESSAGE, NO, NOSORT, PMESSAGE, PSUBSCRIBE, PUNSUBSCRIBE, OK, ONE, QUEUED, SET, STORE, SUBSCRIBE, UNSUBSCRIBE, WEIGHTS, WITHSCORES, RESETSTAT, RESET, FLUSH, EXISTS, LOAD, KILL, LEN, REFCOUNT, ENCODING, IDLETIME, AND, OR, XOR, NOT, GETNAME, SETNAME, LIST, MATCH, COUNT;
		public final byte[] raw;

		Keyword() {
			raw = SafeEncoder.encode(this.name().toLowerCase());
		}
	}
}
