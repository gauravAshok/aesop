package com.flipkart.aesop.runtime.redis.relay.config;

import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;

public class RedisLogicalSourceStaticConfig extends LogicalSourceStaticConfig {

	public static class LogicalGroupCriteria {
		public final String prefix;
		public final String suffix;
		public final String contains;
		
		public LogicalGroupCriteria()
		{
			prefix = suffix = contains = "";
		}
		
		public LogicalGroupCriteria(String prefix, String suffix, String contains) {
	        super();
	        this.prefix = prefix;
	        this.suffix = suffix;
	        this.contains = contains;
        }
		
		/**
		 * Test the given string if it passes the criteria
		 */
		public boolean test(String str) {
			return str.startsWith(prefix) && str.endsWith(suffix) && str.contains(contains);
		}
	}
	
	private final LogicalGroupCriteria groupCriteria;
	
	public RedisLogicalSourceStaticConfig(short id,
										  String name, 
										  String uri, 
										  String partitionFunction, 
										  short partition,
										  boolean skipInfinityScn, 
										  String regularQueryHints, 
										  String chunkedTxnQueryHints, 
										  String chunkedScnQueryHints,
										  LogicalGroupCriteria groupCriteria) {
	    super(id, name, uri, partitionFunction, partition, skipInfinityScn, regularQueryHints, chunkedTxnQueryHints,
	            chunkedScnQueryHints);
	    this.groupCriteria = groupCriteria;
    }

	public LogicalGroupCriteria getGroupCriteria() {
		return groupCriteria;
	}
}
