/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.runtime.redis.producer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.AbstractEventProducer;
import com.flipkart.aesop.runtime.redis.listener.RedisEventListener;
import com.flipkart.aesop.runtime.redis.mapper.CommandEventMapper;
import com.flipkart.aesop.runtime.redis.mapper.KeyValueEventMapper;
import com.flipkart.aesop.runtime.redis.relay.config.RedisPhysicalSourceConfig;
import com.flipkart.aesop.runtime.redis.relay.config.RedisPhysicalSourceStaticConfig;
import com.flipkart.redis.event.CommandEvent;
import com.flipkart.redis.event.KeyValueEvent;
import com.flipkart.redis.event.listener.AbstractEventListener;
import com.flipkart.redis.replicator.RedisReplicator;

public class RedisEventProducer<T extends GenericRecord> extends AbstractEventProducer implements InitializingBean {

	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(RedisEventProducer.class);

	/** Big offset used to offset negative scn from rdb events */
	private static long scnOffset = 100000000000000000L;

	/** The ScnGenerator for generating relayer Scn */
	protected SCNGenerator scnGenerator = new OffsetAwareSCNGenerator(scnOffset);
	
	/** Redis replicator */
	RedisReplicator replicator = null;

	private volatile AtomicBoolean shutdownRequested = new AtomicBoolean(false);

	private AbstractEventListener<CommandEvent> cmdEventListener = null;
	private AbstractEventListener<KeyValueEvent> kvEventListener = null;
	private AbstractEventListener<KeyValueEvent> rdbEventListener = null;

	private CommandEventMapper<T> cmdEventMapper = null;
	private KeyValueEventMapper<T> kvEventMapper = null;

	/**
	 * Interface method implementation. Checks for mandatory dependencies and creates the SEP consumer
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(
		        RedisPhysicalSourceConfig.class.isAssignableFrom(this.physicalSourceConfig.getClass()),
		        "PhysicalSourceConfig must be of type RedisPhysicalSourceConfig");
		RedisPhysicalSourceConfig config = (RedisPhysicalSourceConfig) this.physicalSourceConfig;
		if (config.isFetchFullKeyValueOnUpdate()) {
			Assert.notNull(kvEventMapper, "keyValueEvent mapper is null");
		}
		else {
			Assert.notNull(cmdEventMapper, "CommandEvent mapper is null");
		}
	}

	/**
	 * Interface method implementation. Starts up the SEP consumer
	 * @see com.linkedin.databus2.producers.EventProducer#start(long)
	 */
	public void start(long sinceSCN) {

		URI uri = null;

		try {
			uri = new URI(this.physicalSourceStaticConfig.getUri());
		}
		catch (URISyntaxException e) {
			LOGGER.error("Illegal physical source URI: {}", this.physicalSourceStaticConfig.getUri());
			return;
		}

		replicator = new RedisReplicator(uri);
		replicator.setInitBacklogOffset(sinceSCN);
		replicator.setSoTimeout(10000);

		boolean fetchFullDataOnUpdate =
		        ((RedisPhysicalSourceStaticConfig) physicalSourceStaticConfig).isFetchFullKeyValueOnUpdate();

		if (fetchFullDataOnUpdate) {
			kvEventListener =
			        new RedisEventListener<T, KeyValueEvent>(dbusEventsStatisticsCollector, schemaRegistryService,
			                this.sinceSCN, scnGenerator, this, kvEventMapper);

			replicator.setKeyValueEventListener(kvEventListener);
			replicator.setFetchFullKeyValueOnUpdate(fetchFullDataOnUpdate);
		}
		else {
			cmdEventListener =
			        new RedisEventListener<T, CommandEvent>(dbusEventsStatisticsCollector, schemaRegistryService,
			                this.sinceSCN, scnGenerator, this, cmdEventMapper);

			replicator.setCommandEventListener(cmdEventListener);
		}

		/* if init scn is negative or 0, set rdbEventListener too */
		if (sinceSCN <= 0) {
			rdbEventListener =
			        new RedisEventListener<T, KeyValueEvent>(dbusEventsStatisticsCollector, schemaRegistryService,
			                this.sinceSCN, scnGenerator, this, kvEventMapper);

			replicator.setRdbKeyValueEventListener(rdbEventListener);
		}

		try {
			replicator.start();
			replicator.joinOnReplicationTask();
		}
		catch (Exception e) {
			LOGGER.error("ERROR in while replicating. Err : {}", e);
		}
	}

	/**
	 * Interface method implementation. Stops the SEP consumer
	 * @see com.linkedin.databus2.producers.EventProducer#shutdown()
	 */
	public void shutdown() {
		LOGGER.info("Shutdown has been requested. RedisReplicator shutting down");
		this.shutdownRequested.set(true);
		try {
	        this.replicator.stop(5);
        }
        catch (InterruptedException e) {
	        LOGGER.info("Error while stopping replicator. Err: {}", e);
        }

		super.shutdown();
	}

	public boolean isPaused() {
		return !this.isRunning();
	}

	public boolean isRunning() {
		return this.replicator.isRunning();
	}

	/** Methods that are not supported and therefore throw {@link UnsupportedOperationException} */
	public void pause() {
		throw new UnsupportedOperationException("'pause' is not supported on this event producer");
	}

	public void unpause() {
		throw new UnsupportedOperationException("'unpause' is not supported on this event producer");
	}

	public void waitForShutdown() throws InterruptedException, IllegalStateException {
		throw new UnsupportedOperationException("'waitForShutdown' is not supported on this event producer");
	}

	public void waitForShutdown(long time) throws InterruptedException, IllegalStateException {
		throw new UnsupportedOperationException("'waitForShutdown(long time)' is not supported on this event producer");
	}

	public void setCommandEventMapper(CommandEventMapper<T> commandEventMapper) {
		this.cmdEventMapper = commandEventMapper;
	}

	public void setKvEventMapper(KeyValueEventMapper<T> kvEventMapper) {
		this.kvEventMapper = kvEventMapper;
	}
}
