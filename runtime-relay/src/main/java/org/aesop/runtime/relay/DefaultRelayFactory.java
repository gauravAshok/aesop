/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aesop.runtime.relay;

import java.util.ArrayList;
import java.util.List;

import org.aesop.runtime.config.ProducerRegistration;
import org.aesop.runtime.config.RelayConfig;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.netty.HttpRelay.Config;
import com.linkedin.databus.container.netty.HttpRelay.StaticConfig;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.producers.RelayEventProducersRegistry;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * The Spring factory bean for creating {@link DefaultRelay} instances based on configured properties
 * 
 * @author Regunath B
 * @version 1.0, 16 Jan 2014
 */
public class DefaultRelayFactory  implements FactoryBean<DefaultRelay>, InitializingBean {

	/** The configuration details for creating the Relay*/
	private RelayConfig relayConfig;
	
	/** The ProducerRegistration list for the Relay*/
	private List<ProducerRegistration> producerRegistrationList = new ArrayList<ProducerRegistration>();
	
	/** The event producers registry*/
    private RelayEventProducersRegistry producersRegistry;	
	
    /** The SCN reader-writer*/
    private MultiServerSequenceNumberHandler maxScnReaderWriters;
	
    /**
     * Interface method implementation. Creates and returns a {@link DefaultRelay} instance
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
	public DefaultRelay getObject() throws Exception {
		Config config = new Config();		
		ConfigLoader<StaticConfig> staticConfigLoader = new ConfigLoader<StaticConfig>(RelayConfig.RELAY_PROPERTIES_PREFIX, config);
		HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(this.relayConfig.getRelayProperties());
		
		PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[this.producerRegistrationList.size()];
		for (int i=0; i < this.producerRegistrationList.size(); i++) {
			pStaticConfigs[i] = this.producerRegistrationList.get(i).getPhysicalSourceConfig().build();			
		}
		DefaultRelay relay = new DefaultRelay(staticConfig,pStaticConfigs);
		if (this.producersRegistry == null) {
			this.producersRegistry = RelayEventProducersRegistry.getInstance();
			relay.setProducersRegistry(this.producersRegistry);
		}
		if (this.maxScnReaderWriters == null) {
			SequenceNumberHandlerFactory handlerFactory = staticConfig.getDataSources().getSequenceNumbersHandler().createFactory();
			this.maxScnReaderWriters = new MultiServerSequenceNumberHandler(handlerFactory);
			relay.setMaxScnReaderWriters(this.maxScnReaderWriters);
		}
		return relay;
	}

	/**
	 * Interface method implementation. Checks for mandatory dependencies and initializes this Relay
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.relayConfig,"'relayConfig' cannot be null. This Relay will not be initialized");
		Assert.notEmpty(this.producerRegistrationList,"'producerRegistrationList' cannot be empty. No Event producers registered");		
	}
	
	/**
	 * Interface method implementation. Returns the DefaultRelay type
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<DefaultRelay> getObjectType() {
		return DefaultRelay.class;
	}

	/**
	 * Interface method implementation. Returns true
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */	
	public boolean isSingleton() {
		return true;
	}

	/** Getter/Setter methods to override default implementations of various components used by this Relay*/
	public RelayEventProducersRegistry getProducersRegistry() {
		return this.producersRegistry;
	}
	public void setProducersRegistry(RelayEventProducersRegistry producersRegistry) {
		this.producersRegistry = producersRegistry;
	}
	public MultiServerSequenceNumberHandler getMaxScnReaderWriters() {
		return this.maxScnReaderWriters;
	}
	public void setMaxScnReaderWriters(
			MultiServerSequenceNumberHandler maxScnReaderWriters) {
		this.maxScnReaderWriters = maxScnReaderWriters;
	}
	public RelayConfig getRelayConfig() {
		return this.relayConfig;
	}
	public void setRelayConfig(RelayConfig relayConfig) {
		this.relayConfig = relayConfig;
	}
	public void setProducerRegistrationList(List<ProducerRegistration> producerRegistrationList) {
		this.producerRegistrationList = producerRegistrationList;
	}
	public List<ProducerRegistration> getProducerRegistrationList() {
		return this.producerRegistrationList;
	}
	
}
