![Release status](https://github.com/venasolutions/bosk/actions/workflows/release.yml/badge.svg)
[![Maven Central](https://img.shields.io/maven-central/v/io.vena/bosk-core)](https://mvnrepository.com/artifact/io.vena/bosk-core)

# Bosk

![Three inquisitive cartoon trees with eyes](/art/bosk-evergreen-icon.png)

Bosk is a state management library for developing distributed application control-plane logic.
It's a bit like server-side Redux for Java, but without the boilerplate code.
(No selectors, no action objects, no reducers.)

Bosk eases the transition from a standalone application to a clustered high-availability replica set,
by supporting a programming style that minimizes the surprises encountered during the transition.
Bosk encourages reactive event-triggered closed-loop control logic
based on a user-defined immutable state tree structure,
and favours idempotency and determinism.

State is kept in memory, making reads extremely fast (on the order of 50ns).

Replication is achieved by activating an optional [MongoDB module](bosk-mongo), meaning the hard work of
change propagation, ordering, durability, consistency, atomicity, and observability,
as well as fault tolerance, and emergency manual state inspection and modification,
is all delegated to MongoDB: a well-known, reliable, battle-hardened codebase.
You don't need to trust Bosk to get all these details right:
all we do is send updates to MongoDB, and maintain the in-memory replica by following the MongoDB change stream.

## Documentation
- [User's Guide](docs/USERS.md)

## Getting Started

### Build settings

First, be sure you're compiling Java with the `-parameters` argument.

In Gradle:

```
dependencies {
	compileJava {
		options.compilerArgs << '-parameters'
	}

	compileTestJava {
		options.compilerArgs << '-parameters'
	}
}
```

In Maven:

```
<plugin>
		<groupId>org.apache.maven.plugins</groupId>
		<artifactId>maven-compiler-plugin</artifactId>
		<configuration>
				<compilerArgs>
						<arg>-parameters</arg>
				</compilerArgs>
		</configuration>
</plugin>
```

### Standalone example

The [bosk-core](bosk-core) library is enough to create a `Bosk` object and start writing your application.

The library works particularly well with Java records.
You can define your state tree's root node as follows:

```
import works.bosk.StateTreeNode;
import works.bosk.Identifier;

public record ExampleState (
	// Add fields here as you need them
	String name
) implements StateTreeNode {}
```

You can also use classes, especially if you're using Lombok:

```
@Value
@Accessors(fluent = true)
public class ExampleState implements StateTreeNode {
	// Add fields here as you need them
	String name;
}
```

Now declare your singleton `Bosk` class to house and manage your application state:

```
import works.bosk.Bosk;
import works.bosk.DriverFactory;
import works.bosk.Reference;
import annotations.works.bosk.ReferencePath;
import exceptions.works.bosk.InvalidTypeException;

@Singleton // You can use your framework's dependency injection for this
public class ExampleBosk extends Bosk<ExampleState> {
	public ExampleBosk() throws InvalidTypeException {
		super(
			"ExampleBosk",
			ExampleState.class,
			defaultRoot(),
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

	// Start off simple
	private static DriverFactory<ExampleState> driverFactory() {
		return Bosk::simpleDriver;
	}
}
```

You create an instance of `ExampleBosk` at initialization time,
typically using your application framework's dependency injection system.

To read state, acquire a `ReadContext`:

```
try (var __ = bosk.readContext()) {
	System.out.println("Hello, " + bosk.refs.name.value());
}
```

A read context is intended to be coarse-grained, for example covering an entire HTTP request,
giving you "snapshot-at-start" semantics and protecting you from race conditions.
It is an antipattern to use many small read contexts during the course of a single operation.

To modify state, use the `BoskDriver` interface:

```
bosk.driver().submitReplacement(bosk.refs.name(), "everybody");
```

During your application's initialization, register a hook to perform an action whenever state changes:

```
bosk.registerHook("Name update", bosk.refs.name(), ref -> {
	System.out.println("Name is now: " + ref.value());
});
```

After this, you can add in other packages as you need them,
like [bosk-jackson](bosk-jackson) for JSON serialization.
or [bosk-mongo](bosk-mongo) for persistence and replication.
Use the same version number for all packages.

### Replication

When you're ready to turn your standalone app into a replica set,
add [bosk-mongo](bosk-mongo) as a dependency
and change your Bosk `driverFactory` method to substitute `MongoDriver` in place of `Bosk::simpleDriver`:

```
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import mongo.drivers.works.bosk.BsonPlugin;
import mongo.drivers.works.bosk.MongoDriver;
import mongo.drivers.works.bosk.MongoDriverSettings;

...
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
```

To run this, you'll need a MongoDB replica set.
You can run a single-node replica set using the following `Dockerfile`:

```
FROM mongo:4.4
RUN echo "rs.initiate()" > /docker-entrypoint-initdb.d/rs-initiate.js
CMD [ "mongod", "--replSet", "rsLonesome", "--port", "27017", "--bind_ip_all" ]
```

Now you can run multiple copies of your application, and they will share state.

## Development

### Code Structure

The repo is structured as a collection of subprojects because we publish several separate libraries.
[bosk-core](bosk-core) is the main functionality, and then other packages like [bosk-mongo](bosk-mongo) and [bosk-jackson](bosk-jackson)
provide integrations with other technologies.

The subprojects are listed in [settings.gradle](settings.gradle), and each has its own `README.md` describing what it is.

### Compiler flags

Ensure `javac` is supplied the `-parameters` flag.

This is required because,
for each class you use to describe your Bosk state, the "system of record" for its structure is its constructor.
For example, you might define a class with a constructor like this:

```
public Member(Identifier id, String name) {...}
```

Based on this, Bosk now knows the names and types of all the "properties" of your object.
For this to work smoothly, the parameter names must be present in the compiled bytecode.

### Gradle setup

Each project has its own `build.gradle`.
Common settings across projects are in custom plugins under the [buildSrc directory](buildSrc/src/main/groovy).

### Versioning

In the long run, we'll use the usual semantic versioning.

For the 0.x.y releases, treat x as a major release number.

For the 0.0.x releases, all bets are off, and no backward compatibility is guaranteed.
