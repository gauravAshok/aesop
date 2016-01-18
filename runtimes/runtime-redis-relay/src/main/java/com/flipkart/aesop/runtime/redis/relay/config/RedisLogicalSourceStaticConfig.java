package com.flipkart.aesop.runtime.redis.relay.config;

import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;

public class RedisLogicalSourceStaticConfig extends LogicalSourceStaticConfig {

	public static class LogicalGroupCriteria {
		public String prefix;
		public String suffix;
		public String contains;
		
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
		
		public String getPrefix() {
			return prefix;
		}

		public String getSuffix() {
			return suffix;
		}
		
		public String getContains() {
			return contains;
		}
		
		public void setPrefix(String prefix) {
			this.prefix = prefix;
		}
		
		public void setSuffix(String suffix) {
			this.suffix = suffix;
		}
		
		public void setContains(String contains) {
			this.contains = contains;
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
