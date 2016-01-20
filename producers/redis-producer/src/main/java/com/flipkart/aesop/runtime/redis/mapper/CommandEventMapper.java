package com.flipkart.aesop.runtime.redis.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.CommandArgsPair;
import com.linkedin.databus.core.DbusOpcode;

public class CommandEventMapper<T extends GenericRecord> implements AbstractEventMapper<T, CommandArgsPair> {

	private static final Logger LOGGER = LogFactory.getLogger(CommandEventMapper.class);
	
	@Override
    public T mapEventToGenericRecord(Event<CommandArgsPair> event, DbusOpcode dbusOpCode, Schema schema) {
		
		Schema cmdSchema = schema.getField("command").schema().getTypes().get(0);
		GenericRecord record = new GenericData.Record(schema);
		GenericRecord cmd = new GenericData.Record(cmdSchema);
		
		cmd.put("cmd", event.getData().getCommand());
		cmd.put("args", event.getData().getArgs());
		
		record.put("command", cmd);
		record.put("key", event.getKey());
		record.put("keyvalue", null);
		
		LOGGER.debug("Mapped event to a record : {}", record);
		
		return (T)record;
    }

	@Override
    public String getUniqueName() {
	    return "DefaultCommandEventMapper";
    }

}
