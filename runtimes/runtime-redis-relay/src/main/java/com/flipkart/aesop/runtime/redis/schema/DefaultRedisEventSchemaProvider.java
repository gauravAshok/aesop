package com.flipkart.aesop.runtime.redis.schema;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.ResourceVersionedSchemaSetProvider;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.VersionedSchemaSetProvider;

public class DefaultRedisEventSchemaProvider {
	public static final String defaultRedisEventSchemaPrefix = "com.flipkart.aesop.events.redis.event";

	private static final String namespacePlaceholder = "__NAMESPACE__";
	private static final String namePlaceholder = "__NAME__";

	private String defaultEventSchema = null;

	private VersionedSchemaSetProvider schemaProvider = new ResourceVersionedSchemaSetProvider(null);;
	private static Parser schemaParser = new Parser();

	public DefaultRedisEventSchemaProvider()
	        throws NoSuchSchemaException, DatabusException {
		init();
	}
	
	private void init() throws NoSuchSchemaException, DatabusException {
		/* Load default schema */
		VersionedSchemaSet schemaSet = schemaProvider.loadSchemas();
		defaultEventSchema = schemaSet.getLatestVersionByName(defaultRedisEventSchemaPrefix).getSchema().toString();
	}

	public Schema getDefaultEventSchema(String namespace, String name) {
		return schemaParser.parse(defaultEventSchema.replace(namespacePlaceholder, namespace).replace(
		        namePlaceholder, name));
	}
}
