package com.flipkart.aesop.runtime.redis.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.flipkart.redis.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;

public interface AbstractEventMapper<T extends GenericRecord, U extends AbstractEvent> {
	
	/**
	 * Maps the specified back log event to an appropriate instance of T
	 */
	public T mapEventToGenericRecord(U event, DbusOpcode dbusOpCode, Schema schema);

	/**
	 * Returns a name unique to this mapper type.
	 * @return unique name for this mapper type
	 */
	public String getUniqueName();
}
