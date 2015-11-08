package com.flipkart.aesop.runtime.redis.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.redis.event.CommandEvent;
import com.linkedin.databus.core.DbusOpcode;

public class CommandEventMapper<T extends GenericRecord> implements AbstractEventMapper<T, CommandEvent> {

	private static final Logger LOGGER = LogFactory.getLogger(CommandEventMapper.class);
	
	@Override
    public T mapEventToGenericRecord(CommandEvent event, DbusOpcode dbusOpCode, Schema schema) {
		
		Schema cmdSchema = schema.getField("command").schema().getTypes().get(0);
		GenericRecord record = new GenericData.Record(schema);
		GenericRecord cmd = new GenericData.Record(cmdSchema);
		
		cmd.put("cmd", event.getCommand());
		cmd.put("args", event.getArgs());
		
		record.put("command", cmd);
		record.put("keyvalue", null);
		
		LOGGER.debug("Mapped event to a record : {}", record);
		
		return (T)record;
    }

	@Override
    public String getUniqueName() {
	    return "DefaultCommandEventMapper";
    }

}
