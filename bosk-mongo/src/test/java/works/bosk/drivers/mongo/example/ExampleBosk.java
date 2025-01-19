package works.bosk.drivers.mongo.example;

import com.mongodb.MongoClientSettings;
import works.bosk.Bosk;
import works.bosk.DriverFactory;
import works.bosk.Reference;
import works.bosk.annotations.ReferencePath;
import works.bosk.bson.BsonPlugin;
import works.bosk.drivers.mongo.MongoDriver;
import works.bosk.drivers.mongo.MongoDriverSettings;
import works.bosk.exceptions.InvalidTypeException;

public class ExampleBosk extends Bosk<ExampleState> {
	public ExampleBosk() throws InvalidTypeException {
		super(
			"ExampleBosk",
			ExampleState.class,
			_ -> defaultRoot(),
			driverFactory());
	}

	public interface Refs {
		// Typically, you add a bunch of useful references here, like this one:
		@ReferencePath("/name") Reference<String> name();
	}

	public final Refs refs = rootReference().buildReferences(Refs.class);

	private static ExampleState defaultRoot() {
		return new ExampleState("world");
	}

	private static DriverFactory<ExampleState> driverFactory() {
		MongoClientSettings clientSettings = MongoClientSettings.builder()
			.build();

		MongoDriverSettings driverSettings = MongoDriverSettings.builder()
			.database("exampleDB")
			.build();

		// For advanced usage, you'll want to inject this object,
		// but for getting started, we can just create one here.
		BsonPlugin bsonPlugin = new BsonPlugin();

		return MongoDriver.factory(
			clientSettings,
			driverSettings,
			bsonPlugin);
	}
}
