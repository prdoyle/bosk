package org.vena.bosk.drivers.mongo;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.experimental.Accessors;
import org.vena.bosk.Identifier;

@Value
@Accessors(fluent = true)
@Builder
public class MongoDriverSettings {
	String database;
	String collection;

	@Default long flushTimeoutMS = 30_000;
}
