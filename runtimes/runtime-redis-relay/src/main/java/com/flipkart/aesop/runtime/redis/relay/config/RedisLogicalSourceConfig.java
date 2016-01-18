package com.flipkart.aesop.runtime.redis.relay.config;

import com.flipkart.aesop.runtime.redis.relay.config.RedisLogicalSourceStaticConfig.LogicalGroupCriteria;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;

public class RedisLogicalSourceConfig extends LogicalSourceConfig {

	LogicalGroupCriteria groupCriteria = new LogicalGroupCriteria();

	public LogicalGroupCriteria getGroupCriteria() {
		return groupCriteria;
	}

	public void setGroupCriteria(LogicalGroupCriteria groupCriteria) {
		this.groupCriteria = groupCriteria;
	}

	@Override
	public LogicalSourceStaticConfig build() throws InvalidConfigException {
		checkForNulls();
		if(groupCriteria == null) {
			groupCriteria = new LogicalGroupCriteria();
		}
		return new RedisLogicalSourceStaticConfig(this.getId(),
												  this.getName(),
												  this.getUri(), 
												  this.getPartitionFunction(),
												  this.getPartition(),
												  this.isSkipInfinityScn(),
												  this.getRegularQueryHints(),
												  this.getChunkedTxnQueryHints(),
												  this.getChunkedScnQueryHints(),
												  groupCriteria);
	}
	
	public void setKeyPrefix(String prefix) {
		this.groupCriteria.setPrefix(prefix);
	}
	
	public void setKeySuffix(String suffix) {
		this.groupCriteria.setSuffix(suffix);
	}
	
	public void setKeyContains(String contains) {
		this.groupCriteria.setContains(contains);
	}
}
