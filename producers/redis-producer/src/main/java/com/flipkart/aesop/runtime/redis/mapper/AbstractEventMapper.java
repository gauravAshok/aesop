package com.flipkart.aesop.runtime.redis.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.AbstractData;
import com.linkedin.databus.core.DbusOpcode;

public interface AbstractEventMapper<T extends GenericRecord, U extends AbstractData> {
	
	/**
	 * Maps the specified back log event to an appropriate instance of T
	 */
	public T mapEventToGenericRecord(Event<U> event, DbusOpcode dbusOpCode, Schema schema);

	/**
	 * Returns a name unique to this mapper type.
	 * @return unique name for this mapper type
	 */
	public String getUniqueName();
}
