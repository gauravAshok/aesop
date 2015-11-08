package com.flipkart.aesop.runtime.redis.relay.config;

import java.util.List;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig.ChunkingType;

public class RedisPhysicalSourceConfig extends PhysicalSourceConfig {

	private boolean fetchFullKeyValueOnUpdate;

	public RedisPhysicalSourceConfig() {
		super();
		fetchFullKeyValueOnUpdate = false;
	}

	public boolean isFetchFullKeyValueOnUpdate() {
		return fetchFullKeyValueOnUpdate;
	}

	public void setFetchFullKeyValueOnUpdate(boolean fetchFullKeyValueOnUpdate) {
		this.fetchFullKeyValueOnUpdate = fetchFullKeyValueOnUpdate;
	}

	private LogicalSourceConfig addOrGetSource(int index) {
		List<LogicalSourceConfig> sources = getSources();
		if (index >= sources.size()) {
			for (int i = sources.size(); i <= index; ++i) {
				sources.add(new RedisLogicalSourceConfig());
			}
		}

		return sources.get(index);
	}

	@Override
	public LogicalSourceConfig getSource(int index) {
		LogicalSourceConfig result = addOrGetSource(index);
		return result;
	}

	@Override
	public void setSource(int index, LogicalSourceConfig source) {
		if(source instanceof RedisLogicalSourceConfig) {
			addOrGetSource(index);
			getSources().set(index, source);
		}
		else {
			throw new IllegalArgumentException("can only add RedisLogicalSourceConfig");
		}
	}

	@Override
	public void addSource(LogicalSourceConfig source) {
		if(source instanceof RedisLogicalSourceConfig) {
			if (!getSources().contains(source))
				getSources().add(source);
		}
		else {
			throw new IllegalArgumentException("can only add RedisLogicalSourceConfig");
		}
	}
	
	@Override
	  public PhysicalSourceStaticConfig build() throws InvalidConfigException
	  {
	    checkForNulls();
	    //check config options for chained relays
	    if (this.getLargestEventSizeInBytes() >= this.getLargestWindowSizeInBytes())
	    {
	      throw new InvalidConfigException("Invalid relay config: largestEventSizeInBytes has to be lesser than largestWindowSizeInBytes:"
	          + " largestEventSizeInBytes=" + this.getLargestEventSizeInBytes() + " largestWindowSizeInBytes=" + this.getLargestWindowSizeInBytes());
	    }

	    LogicalSourceStaticConfig[] sourcesStaticConfigs = new LogicalSourceStaticConfig[this.getSources().size()];
	    for (int i = 0 ; i < this.getSources().size(); ++i)
	    {
	      sourcesStaticConfigs[i] = this.getSources().get(i).build();
	    }
	    ChunkingType chunkingType = ChunkingType.valueOf(this.getChunkingType());
	    return new RedisPhysicalSourceStaticConfig(this.getName(),
	    										this.getId(),
	    										this.getUri(),
	    										this.getResourceKey(),
	                                            sourcesStaticConfigs,
	                                            this.getRole(),
	                                            this.getSlowSourceQueryThreshold(),
	                                            this.getRestartScnOffset(),
	                                            this.getRetries().build(),
	                                            chunkingType,
	                                            this.getTxnsPerChunk(),
	                                            this.getScnChunkSize(),
	                                            this.getChunkedScnThreshold(),
	                                            this.getMaxScnDelayMs(),
	                                            this.getEventRatePerSec(),
	                                            this.getMaxThrottleDurationInSecs(),
	                                            isDbusEventBufferSet() ? this.getDbusEventBuffer().build():null,
	                                            this.getLargestEventSizeInBytes(),
	                                            this.getLargestWindowSizeInBytes(),
	                                            this.getErrorOnMissingFields(),
	                                            this.getXmlVersion(),
	                                            this.getXmlEncoding(),
	                                            this.getReplBitSetter().build(),
	                                            fetchFullKeyValueOnUpdate);
	  }
}
