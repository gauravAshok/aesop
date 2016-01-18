package com.flipkart.aesop.runtime.redis.relay.config;

import com.linkedin.databus.core.DbusEventBuffer.StaticConfig;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;

public class RedisPhysicalSourceStaticConfig extends PhysicalSourceStaticConfig {

	private final boolean fetchFullDataOnKeyUpdate;
	
	public RedisPhysicalSourceStaticConfig(String name,
										int id,
										String uri,
										String resourceKey,
										LogicalSourceStaticConfig[] sources,
										String role,
										long slowSourceQueryThreshold,
										long restartScnOffset,
										BackoffTimerStaticConfig errorRetries,
										ChunkingType chunkingType,
										long txnsPerChunk,
										long scnChunkSize,
										long chunkedScnThreshold,
										long maxScnDelayMs,
										long eventRatePerSec,
										long maxThrottleDurationInSecs,
										StaticConfig dbusEventBuffer,
										int largestEventSizeInBytes,
										long largestWindowSizeInBytes,
										boolean errorOnMissingFields,
										String xmlVersion,
										String xmlEncoding,
										ReplicationBitSetterStaticConfig replicationBitSetter,
										boolean fetchFullDataOnKeyUpdate) {
	    
		super(name, id, uri, resourceKey, sources, role, slowSourceQueryThreshold, restartScnOffset, errorRetries,
	            chunkingType, txnsPerChunk, scnChunkSize, chunkedScnThreshold, maxScnDelayMs, eventRatePerSec,
	            maxThrottleDurationInSecs, dbusEventBuffer, largestEventSizeInBytes, largestWindowSizeInBytes,
	            errorOnMissingFields, xmlVersion, xmlEncoding, replicationBitSetter);
		this.fetchFullDataOnKeyUpdate = fetchFullDataOnKeyUpdate;
	}

	public boolean isFetchFullDataOnKeyUpdate() {
		return fetchFullDataOnKeyUpdate;
	}
	
	@Override
	public String toString() {
		return super.toString() + ";fetchFullKey=" + fetchFullDataOnKeyUpdate;
	}
}