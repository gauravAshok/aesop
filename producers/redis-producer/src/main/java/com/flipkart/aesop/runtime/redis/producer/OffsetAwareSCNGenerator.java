package com.flipkart.aesop.runtime.redis.producer;

import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;

public class OffsetAwareSCNGenerator implements SCNGenerator {
	
	private long offset;
	
	/**
	 * Returns the local SCN after adding an offset. It ignores the host identifier.
	 * @see com.flipkart.aesop.runtime.producer.SCNGenerator#getSCN(long, java.lang.String)
	 */
	
	public OffsetAwareSCNGenerator(long offset) {
		this.offset = offset;
	}
	
	public long getSCN(long localSCN, String hostId) {
		return localSCN + offset;
	}
	
	public long getOffset() {
		return offset;
	}
}
