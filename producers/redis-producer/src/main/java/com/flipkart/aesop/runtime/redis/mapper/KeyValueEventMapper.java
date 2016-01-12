package com.flipkart.aesop.runtime.redis.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.KeyValuePair;
import com.linkedin.databus.core.DbusOpcode;

public class KeyValueEventMapper<T extends GenericRecord> implements AbstractEventMapper<T, KeyValuePair> {

	private static final Logger LOGGER = LogFactory.getLogger(KeyValueEventMapper.class);
	
	@Override
    public T mapEventToGenericRecord(Event<KeyValuePair> event, DbusOpcode dbusOpCode, Schema schema) {
		Schema keyvalueSchema = schema.getField("keyvalue").schema().getTypes().get(0);
		GenericRecord record = new GenericData.Record(schema);
		GenericRecord keyValuePair = new GenericData.Record(keyvalueSchema);
		
		keyValuePair.put("key", event.getKey());
		keyValuePair.put("value", event.getData().getValue());
		keyValuePair.put("database", event.getData().getDatabase());
		keyValuePair.put("datatype", event.getData().getType().name());
		
		record.put("keyvalue", keyValuePair);
		record.put("command", null);
		
		LOGGER.debug("Mapped event to a record : {}", record);
		
		return (T)record;
    }

	@Override
    public String getUniqueName() {
	    return "DefaultKeyValueEventMapper";
    }
}
