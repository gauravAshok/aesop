package com.flipkart.redis.net;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;

import com.flipkart.redis.event.CommandEvent;
import com.flipkart.redis.net.Protocol.Command;
import com.flipkart.redis.net.rdb.RDBParser;
import com.flipkart.redis.net.rdb.RDBParser.Entry;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.IOUtils;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;
import redis.clients.util.SafeEncoder;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.observables.ConnectableObservable;

public class Connection implements Closeable {

	private static final byte[][] EMPTY_ARGS = new byte[0][];

	private String host = Protocol.DEFAULT_HOST;
	private int port = Protocol.DEFAULT_PORT;
	private Socket socket;
	private RedisOutputStream outputStream;
	private RedisInputStream inputStream;
	private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	private int soTimeout = Protocol.DEFAULT_TIMEOUT;
	private boolean broken = false;

	public Connection() {
	}

	public Connection(final String host) {
		this.host = host;
	}

	public Connection(final String host, final int port) {
		this.host = host;
		this.port = port;
	}

	public Socket getSocket() {
		return socket;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public int getSoTimeout() {
		return soTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public void setSoTimeout(int soTimeout) {
		this.soTimeout = soTimeout;
	}

	public void setTimeoutInfinite() {
		try {
			if (!isConnected()) {
				connect();
			}
			socket.setSoTimeout(0);
		}
		catch (SocketException ex) {
			broken = true;
			throw new JedisConnectionException(ex);
		}
	}

	public void rollbackTimeout() {
		try {
			socket.setSoTimeout(soTimeout);
		}
		catch (SocketException ex) {
			broken = true;
			throw new JedisConnectionException(ex);
		}
	}

	public Connection sendCommand(final Command cmd, final String... args) {
		final byte[][] bargs = new byte[args.length][];
		for (int i = 0; i < args.length; i++) {
			bargs[i] = SafeEncoder.encode(args[i]);
		}
		return sendCommand(cmd, bargs);
	}

	public Connection sendCommand(final Command cmd) {
		return sendCommand(cmd, EMPTY_ARGS);
	}

	public Connection sendCommand(final Command cmd, final byte[]... args) {
		try {
			connect();
			Protocol.sendCommand(outputStream, cmd, args);
			return this;
		}
		catch (JedisConnectionException ex) {
			/*
			 * When client send request which formed by invalid protocol, Redis
			 * send back error message before close connection. We try to read
			 * it to provide reason of failure.
			 */
			try {
				String errorMessage = Protocol.readErrorLineIfPossible(inputStream);
				if (errorMessage != null && errorMessage.length() > 0) {
					ex = new JedisConnectionException(errorMessage, ex.getCause());
				}
			}
			catch (Exception e) {
				/*
				 * Catch any IOException or JedisConnectionException occurred
				 * from InputStream#read and just ignore. This approach is safe
				 * because reading error message is optional and connection will
				 * eventually be closed.
				 */
			}
			// Any other exceptions related to connection?
			broken = true;
			throw ex;
		}
	}

	public String getHost() {
		return host;
	}

	public void setHost(final String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(final int port) {
		this.port = port;
	}

	public void connect() {
		if (!isConnected()) {
			try {
				socket = new Socket();
				// ->@wjw_add
				socket.setReuseAddress(true);
				socket.setKeepAlive(true); // Will monitor the TCP connection is
				// valid
				socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to
				// ensure timely delivery of data
				socket.setSoLinger(true, 0); // Control calls close () method,
				// the underlying socket is closed
				// immediately
				// <-@wjw_add

				socket.connect(new InetSocketAddress(host, port), connectionTimeout);
				socket.setSoTimeout(soTimeout);
				outputStream = new RedisOutputStream(socket.getOutputStream());
				inputStream = new RedisInputStream(socket.getInputStream());
			}
			catch (IOException ex) {
				broken = true;
				throw new JedisConnectionException(ex);
			}
		}
	}

	protected String sendLocalPort() {
		return sendCommand(Command.REPLCONF, "listening-port", "" + getSocket().getLocalPort()).getStatusCodeReply().object;
	}

	public String authenticate(String password) {
		return sendCommand(Command.AUTH, password).getStatusCodeReply().object;
	}

	public String requestForPSync(String masterRunId, long initBacklogOffset) {

		// notify master server this connections port.
		sendLocalPort();

		// request psync
		sendCommand(Command.PSYNC, masterRunId, "" + initBacklogOffset);
		return getStatusCodeReply().object;
	}

	public void sendReplAck(long offset) {
		sendCommand(Command.REPLCONF, "ACK", String.valueOf(offset));
	}

	@Override
	public void close() {
		disconnect();
	}

	public void disconnect() {
		if (isConnected()) {
			try {
				outputStream.flush();
				socket.close();
			}
			catch (IOException ex) {
				broken = true;
				throw new JedisConnectionException(ex);
			}
			finally {
				IOUtils.closeQuietly(socket);
			}
		}
	}

	public boolean isConnected() {
		return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
		        && !socket.isInputShutdown() && !socket.isOutputShutdown();
	}

	public Observable<RDBParser.Entry> getRdbDump() {

		final RDBParser rdbParser = new RDBParser();
		rdbParser.init(inputStream);

		Observable<RDBParser.Entry> dataEvents = Observable.create(new OnSubscribe<RDBParser.Entry>() {

			@Override
			public void call(Subscriber<? super Entry> t) {
				try {
					RDBParser.Entry entry = rdbParser.next();

					while (entry != null) {
						t.onNext(entry);
						entry = rdbParser.next();
					}

					byte[] checksum = new byte[8];
					inputStream.read(checksum, 0, 8);

					t.onCompleted();
				}
				catch (Exception e) {
					// error has occurred while parsing the stream. stop
					// emitting any more events
					t.onError(e);
				}
			}
		});

		return dataEvents;
	}

	public Observable<Reply<List<String>>> getCommands() {
		// TODO think about timeouts and exception handling.

		Observable<Reply<List<String>>> cmdEvents = Observable.create(new OnSubscribe<Reply<List<String>>>() {
			@Override
			public void call(Subscriber<? super Reply<List<String>>> t) {
				try {
					while (true) {
						Reply<List<String>> command = getMultiBulkReplySafe();
						t.onNext(command);
					}

				}
				catch (JedisConnectionException e) {
					t.onError(e);
				}
				catch (JedisDataException e) {
					t.onError(e);
				}
			}
		});
		return cmdEvents;
	}

	public String getInfo(String infoFor) {
		if (infoFor == null || infoFor.isEmpty()) {
			sendCommand(Command.INFO);
		}
		else {
			sendCommand(Command.INFO, infoFor);
		}

		return getStatusCodeReply().object;
	}

	protected Reply<String> getStatusCodeReply() {
		flush();
		final Reply<?> status = readProtocolWithCheckingBroken();
		if (null == status) {
			return null;
		}
		return new Reply<String>(SafeEncoder.encode((byte[]) status.object), status.bytesRead);
	}

	protected Reply<String> getBulkReply() {
		final Reply<?> result = getBinaryBulkReply();
		if (null != result) {
			return new Reply<String>(SafeEncoder.encode((byte[]) result.object), result.bytesRead);
		}
		else {
			return null;
		}
	}

	@SuppressWarnings ("unchecked")
	protected Reply<byte[]> getBinaryBulkReply() {
		flush();

		return (Reply<byte[]>) readProtocolWithCheckingBroken();
	}

	@SuppressWarnings ("unchecked")
	protected Reply<Long> getIntegerReply() {
		flush();
		return (Reply<Long>) readProtocolWithCheckingBroken();
	}

	protected Reply<List<String>> getMultiBulkReply() {
		Reply<List<byte[]>> result = getBinaryMultiBulkReply();
		return new Reply<List<String>>(BuilderFactory.STRING_LIST.build(result.object), result.bytesRead);
	}

	protected Reply<List<String>> getMultiBulkReplySafe() {
		Reply<List<byte[]>> result = getBinaryMultiBulkReplySafe();
		return new Reply<List<String>>(BuilderFactory.STRING_LIST.build(result.object), result.bytesRead);
	}

	@SuppressWarnings ("unchecked")
	protected Reply<List<byte[]>> getBinaryMultiBulkReply() {
		flush();
		return (Reply<List<byte[]>>) readProtocolWithCheckingBroken();
	}

	@SuppressWarnings ("unchecked")
	protected Reply<List<byte[]>> getBinaryMultiBulkReplySafe() {
		flush();
		return (Reply<List<byte[]>>) Protocol.read(inputStream, Protocol.ASTERISK_BYTE);
	}

	@SuppressWarnings ("unchecked")
	protected Reply<List<Object>> getRawObjectMultiBulkReply() {
		return (Reply<List<Object>>) readProtocolWithCheckingBroken();
	}

	protected Reply<List<Object>> getObjectMultiBulkReply() {
		flush();
		return getRawObjectMultiBulkReply();
	}

	@SuppressWarnings ("unchecked")
	protected Reply<List<Long>> getIntegerMultiBulkReply() {
		flush();
		return (Reply<List<Long>>) readProtocolWithCheckingBroken();
	}

	@SuppressWarnings ("unchecked")
	protected Reply<Object> getOne() {
		flush();
		return (Reply<Object>) readProtocolWithCheckingBroken();
	}

	private boolean isBroken() {
		return broken;
	}

	protected void flush() {
		try {
			outputStream.flush();
		}
		catch (IOException ex) {
			broken = true;
			throw new JedisConnectionException(ex);
		}
	}

	protected Reply<?> readProtocolWithCheckingBroken() {
		try {
			return Protocol.read(inputStream);
		}
		catch (JedisConnectionException exc) {
			broken = true;
			throw exc;
		}
	}
}
