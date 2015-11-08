package com.flipkart.aesop.runtime.redis.relay;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;

import com.flipkart.aesop.runtime.config.ProducerRegistration;
import com.flipkart.aesop.runtime.config.RelayConfig;
import com.flipkart.aesop.runtime.producer.AbstractEventProducer;
import com.flipkart.aesop.runtime.producer.ProducerEventBuffer;
import com.flipkart.aesop.runtime.relay.DefaultRelay;
import com.flipkart.aesop.runtime.relay.DefaultRelayFactory;
import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSetBackedRegistryService;
import com.flipkart.aesop.runtime.redis.schema.*;

public class RedisRelayFactory extends DefaultRelayFactory {

	@Override
	public DefaultRelay getObject() throws Exception {

		HttpRelay.Config config = new HttpRelay.Config();
		ConfigLoader<HttpRelay.StaticConfig> staticConfigLoader =
		        new ConfigLoader<HttpRelay.StaticConfig>(RelayConfig.RELAY_PROPERTIES_PREFIX, config);
		List<ProducerRegistration> producerRegisterationList = this.getProducerRegistrationList();

		PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[producerRegisterationList.size()];
		for (int i = 0; i < producerRegisterationList.size(); i++) {
			pStaticConfigs[i] = producerRegisterationList.get(i).getPhysicalSourceConfig().build();
			// Register all sources with the static config
			for (LogicalSourceConfig logicalSourceConfig : producerRegisterationList.get(i).getPhysicalSourceConfig()
			        .getSources()) {
				config.setSourceName(String.valueOf(logicalSourceConfig.getId()), logicalSourceConfig.getName());
			}
		}

		// Making this a list to initialise each producer seperately with initial SCN
		HttpRelay.StaticConfig[] staticConfigList = new HttpRelay.StaticConfig[producerRegisterationList.size()];
		DefaultRelay relay = null;

		FileSystemSchemaRegistryService.Config configBuilder = new FileSystemSchemaRegistryService.Config();
		configBuilder.setFallbackToResources(true);
		configBuilder.setSchemaDir(this.getRelayConfig().getSchemaRegistryLocation());
		FileSystemSchemaRegistryService.StaticConfig schemaRegistryServiceConfig = configBuilder.build();

		SchemaRegistryService schemaRegistryService = null;

		try {
			/* try loading schemas from directory */
			schemaRegistryService = FileSystemSchemaRegistryService.build(schemaRegistryServiceConfig);
		}
		catch (DatabusRuntimeException e) {
			/*
			 * if failed, create a empty schemaSet backed registry service which will be later populated with default
			 * schema for logical sources
			 */
			schemaRegistryService = new VersionedSchemaSetBackedRegistryService();
		}
		
		/*
		 * for every logical source for which schema is not found, create a default schema. 
		 */
		DefaultRedisEventSchemaProvider defaultSchemaProvider =
		        new DefaultRedisEventSchemaProvider();
		for (ProducerRegistration producer : producerRegisterationList) {
			for (LogicalSourceConfig logicalSource : producer.getPhysicalSourceConfig().getSources()) {
				if (schemaRegistryService.fetchLatestVersionedSchemaBySourceName(logicalSource.getName()) == null) {

					String[] uriTokens = logicalSource.getUri().split("\\.");
					Schema defaultSchema = defaultSchemaProvider.getDefaultEventSchema(uriTokens[0], uriTokens[1]);

					schemaRegistryService.registerSchema(new VersionedSchema(logicalSource.getName(), (short) 0,
					        defaultSchema, null));
				}
			}
		}

		if (this.getMaxScnReaderWriters() == null) {
			this.setMaxScnReaderWriters(new HashMap<String, MultiServerSequenceNumberHandler>());
			for (int i = 0; i < producerRegisterationList.size(); i++) {
				// Get Properties from Relay Config
				Properties mergedProperties = new Properties();
				mergedProperties.putAll(this.getRelayConfig().getRelayProperties());

				// Obtain Properties from Product Registration if it exists
				if (getProducerRegistrationList().get(i).getProperties() != null) {
					mergedProperties.putAll(getProducerRegistrationList().get(i).getProperties());
				}

				// Loading a list of static configs
				staticConfigList[i] = staticConfigLoader.loadConfig(mergedProperties);

				// Making a handlerFactory per producer.
				SequenceNumberHandlerFactory handlerFactory =
				        staticConfigList[i].getDataSources().getSequenceNumbersHandler().createFactory();
				this.getMaxScnReaderWriters().put(producerRegisterationList.get(i).getPhysicalSourceConfig().getName(),
				        new MultiServerSequenceNumberHandler(handlerFactory));
			}
		}
		/*
		 * Initialising relay. Only passing the first static config as everything else apart from
		 * initial SCN per producer is the same. Initial SCN per producer has already been set
		 */
		relay =
		        new DefaultRelay(staticConfigList[0], pStaticConfigs,
		                SourceIdNameRegistry.createFromIdNamePairs(staticConfigList[0].getSourceIds()),
		                schemaRegistryService);

		/*
		 * Commenting out this line. The {@link #getMaxScnReaderWriters() getMaxScnReaderWriters} is not used anywhere.
		 * relay.setMaxScnReaderWriters(this.maxScnReaderWriters.get(this.producerRegistrationList.get(0)));
		 */

		/* now set all the Relay initialized services on the producers, if they are of type AbstractEventProducer */
		for (int i = 0; i < producerRegisterationList.size(); i++) {
			ProducerRegistration producerRegistration = producerRegisterationList.get(i);
			PhysicalSourceStaticConfig pStaticConfig = pStaticConfigs[i];
			if (AbstractEventProducer.class.isAssignableFrom(producerRegistration.getEventProducer().getClass())) {
				AbstractEventProducer producer = (AbstractEventProducer) producerRegistration.getEventProducer();

				// here we assume single event buffer is shared among all logical sources
				DbusEventBufferAppendable eb =
				        relay.getEventBuffer().getDbusEventBufferAppendable(pStaticConfig.getSources()[0].getId());
				producer.setEventBuffer(new ProducerEventBuffer(producer.getName(), eb, relay.getMetricsCollector()));

				/* Setting the maxScnReaderWriter per producer as initialised above. */
				producer.setMaxScnReaderWriter(this.getMaxScnReaderWriters()
				        .get(producerRegistration.getPhysicalSourceConfig().getName())
				        .getOrCreateHandler(pStaticConfig.getPhysicalPartition()));
				producer.setSchemaRegistryService(relay.getSchemaRegistryService());
				producer.setDbusEventsStatisticsCollector(relay.getInboundEventStatisticsCollector());
			}
		}
		/* set the ProducerRegistration instances on the Relay */
		relay.setProducerRegistrationList(producerRegisterationList);

		return relay;
	}
}
