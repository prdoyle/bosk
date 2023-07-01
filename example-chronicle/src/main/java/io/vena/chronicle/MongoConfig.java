package io.vena.chronicle;

import java.util.Collection;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;

import static java.util.Collections.singleton;

@Configuration
public class MongoConfig extends AbstractMongoClientConfiguration {
	@Override
	protected String getDatabaseName() {
		return "chronicle";
	}

	@Override
	protected Collection<String> getMappingBasePackages() {
		return singleton("io.vena.chronicle.dto");
	}
}
