package com.flipkart.aesop.runtime.redis.listener;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;
import com.flipkart.aesop.runtime.redis.mapper.AbstractEventMapper;
import com.flipkart.aesop.runtime.redis.producer.RedisEventProducer;
import com.flipkart.aesop.runtime.redis.relay.config.RedisLogicalSourceStaticConfig;
import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.AbstractData;
import com.flipkart.redis.event.listener.AbstractEventListener;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.KeyPair;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;

public class RedisEventListener<T extends GenericRecord, U extends AbstractData> implements AbstractEventListener<Event<U>> {

	private static final Logger LOGGER = LogFactory.getLogger(RedisEventListener.class);
	
	DbusEventBufferAppendable eventBuffer;
	MaxSCNReaderWriter maxScnReaderWriter;
    DbusEventsStatisticsCollector dbusEventsStatisticsCollector;
    SchemaRegistryService schemaRegistryService;
    AtomicLong sinceSCN; 
    SCNGenerator scnGenerator;
    RedisEventProducer<T> redisEventProducer;
    AbstractEventMapper<T, U> eventMapper;
    

	public RedisEventListener(DbusEventsStatisticsCollector dbusEventsStatisticsCollector, SchemaRegistryService schemaRegistryService,
            AtomicLong sinceSCN, SCNGenerator scnGenerator, RedisEventProducer<T> redisEventProducer, AbstractEventMapper<T, U> eventMapper) {
	    super();
	    this.eventBuffer = redisEventProducer.getEventBuffer();
	    this.maxScnReaderWriter = redisEventProducer.getMaxScnReaderWriter();
	    this.dbusEventsStatisticsCollector = dbusEventsStatisticsCollector;
	    this.schemaRegistryService = schemaRegistryService;
	    this.sinceSCN = sinceSCN;
	    this.scnGenerator = scnGenerator;
	    this.redisEventProducer = redisEventProducer;
	    
	    this.eventMapper = eventMapper;
    }

	@Override
    public void onEvent(Event<U> event) {
		
	    /* get the logical source */
		if(event != null) {
			String key = event.getKey();
			long currScn =  scnGenerator.getSCN(event.getHeader().getMasterBacklogOffset(), null);
			
			LogicalSourceStaticConfig[] logicalSources = redisEventProducer.getPhysicalSourceStaticConfig().getSources();
			
			RedisLogicalSourceStaticConfig matchedLogicalSource = null;
			
			for(int i = 0; i < logicalSources.length; ++i) {
				RedisLogicalSourceStaticConfig redisLogicalSource = (RedisLogicalSourceStaticConfig)logicalSources[i];
				
				if(redisLogicalSource.getGroupCriteria().test(key)) {
					matchedLogicalSource = redisLogicalSource;
					break;
				}
			}
			
			if(matchedLogicalSource != null) {
				
				Schema schema = null;
				try {
				/* get schema for this logical source and create dbChangeEntry */
					schema = schemaRegistryService.fetchLatestVersionedSchemaBySourceName(matchedLogicalSource.getName()).getSchema();
				}
				catch (Exception e) {
					LOGGER.error("Error getting default schema for {}", matchedLogicalSource.getUri());
					return;
				}
				
				/* create dbChange entry */
				GenericRecord record = eventMapper.mapEventToGenericRecord(event, DbusOpcode.UPSERT,schema);
				
				List<KeyPair> keyPairList = Collections.singletonList(new KeyPair(key, Type.STRING));
				
				DbChangeEntry dbChangeEntry =
				        new DbChangeEntry(currScn, event.getHeader().getTimestamp(), record, DbusOpcode.UPSERT, false, schema,
				                keyPairList);
				
				short pSourceId = (short)redisEventProducer.getPhysicalSourceStaticConfig().getId();
				short lSourceId = matchedLogicalSource.getId();
				
				/* append to dbus */
				int appended = 0;
				eventBuffer.startEvents();
				try {
					appended = createAndAppendEvent(dbChangeEntry, false, pSourceId, lSourceId);
					if(appended == -1) {
						throw new EventCreationException("Failed to append to dbus");
					}
				}
				catch(Exception e) {
					LOGGER.error("Failed to append. msg: {}. entry: {}", e.getMessage(), dbChangeEntry);
				}
				
		        eventBuffer.endEvents(currScn, dbusEventsStatisticsCollector);
		        
		        /* if event was not appended, return */
		        if(appended == -1) {
		        	return;
		        }
				
		        /* update scn */
                try {
	                maxScnReaderWriter.saveMaxScn(currScn);
	                sinceSCN.set(currScn);
                }
                catch (DatabusException e) {
	                LOGGER.error("Failed to update scn. msg: {}", e.getMessage());
                }
			}
			else {
				LOGGER.info("Events recieved from uninterested sources {}", key);
				return;
			}
		}
		else {
			LOGGER.info("Recieved null event");
		}
    }

	@Override
    public void onException(Throwable e) {
	    LOGGER.error("Error in replicator: {}", e);
    }
	
	public int createAndAppendEvent(DbChangeEntry changeEntry, boolean enableTracing, short pSourceId, short lSourceId) throws UnsupportedKeyException, EventCreationException { 
		
		DbusEventKey eventKey = new DbusEventKey(changeEntry.getPkeys().get(0).getKey());
		SchemaId schemaId = SchemaId.createWithMd5(changeEntry.getSchema());
		byte[] payload = serializeEvent(changeEntry.getRecord());
		
		DbusEventInfo eventInfo =
		        new DbusEventInfo(changeEntry.getOpCode(), changeEntry.getScn(), (short) pSourceId, (short) pSourceId,
		                changeEntry.getTimestampInNanos(), (short) lSourceId, schemaId.getByteArray(), payload,
		                enableTracing, false);
		boolean success = eventBuffer.appendEvent(eventKey, eventInfo, dbusEventsStatisticsCollector);
		LOGGER.debug("Append Success: {} . event: {}", success, changeEntry);
		return success ? payload.length : -1;
	}
	
	/**
	 * Serializes avro record into byte array
	 * @param record generic record
	 * @return serialized byte array
	 * @throws EventCreationException Thrown when event creation failed for a databus source
	 */
	protected byte[] serializeEvent(GenericRecord record) throws EventCreationException
	{
		byte[] serializedValue;
		ByteArrayOutputStream bos = null;
		try
		{
			bos = new ByteArrayOutputStream();
			Encoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
			writer.write(record, encoder);
			serializedValue = bos.toByteArray();
		}
		catch (IOException ex)
		{
			LOGGER.error("Failed to serialize avro record : " + record + " Exception : " + ex.getMessage()
			        + "  Cause: " + ex.getCause());
			throw new EventCreationException("Failed to serialize the Avro GenericRecord", ex);
		}
		catch (RuntimeException ex)
		{
			LOGGER.error("Failed to serialize avro record : " + record + " Exception : " + ex.getMessage()
			        + "  Cause: " + ex.getCause());
			throw new EventCreationException("Failed to serialize the Avro GenericRecord", ex);
		}
		finally
		{
			if (bos != null)
			{
				try
				{
					bos.close();
				}
				catch (IOException e)
				{
					LOGGER.error("Exception occurred while closing output stream");
				}
			}
		}
		return serializedValue;
	}
}
