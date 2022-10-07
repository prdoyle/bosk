Import `bosk-core`.

Make your state root node. It must be an Entity.
Give it a field for the particular service within your app:

```java
record DemoState (
	Identifier id,
	MyServiceState myService
) implements Entity {}

record MyServiceState (
	String someSetting
){}
```

Make your subclass of `Bosk`.
Add handy references as desired:

```java
@Singleton
class DemoBosk extends Bosk<DemoState> {
	public DemoBosk() throws InvalidTypeException {
		super("Demo bosk", DemoState.class, DemoBosk::defaultRoot, driverFactory());
	}

    private static DemoState defaultRoot(Bosk<DemoState> testRootBosk) {
		return new DemoState(
            // Constructor parameters here
		);
	}

	private static DriverFactory<TestEntity> driverFactory() {
		return Bosk::simpleDriver;
	}

	public final Reference<String> someSettingRef = reference(String.class, Path.parse(
		"myService/someSetting"));
}
```

In your service, register a hook that responds to changes in the bosk:

```java
@Singleton
static class MyService {
    final DemoBosk bosk;
    volatile String setting;

    MyService(DemoBosk bosk) {
        this.bosk = bosk;

        bosk.registerHook("Update setting", bosk.someSettingRef, ref -> {
            String newValue = ref.valueIfExists();
            if (!Objects.equals(setting, newValue)) {
                LOGGER.info("Setting changed from {} to {}", setting, newValue);
                setting = newValue;
            }
        });
    }
}
```

Wherever appropriate, perform the update itself:

```
bosk.driver().submitReplacement(bosk.someSettingRef, "new value!");
```

Test the application in standalone mode.

When ready, bring in the `bosk-mongo` package and change your Bosk driver initialization to use `MongoDriver`:

```
private static DriverFactory<TestEntity> driverFactory() {
    MongoClientSettings clientSettings = MongoClientSettings.builder()
        .writeConcern(WriteConcern.MAJORITY)
        .readConcern(ReadConcern.MAJORITY)
        .applyToClusterSettings(builder -> {
            builder.hosts(singletonList(new ServerAddress("localhost", 27017)));
        })
        .build();
    MongoDriverSettings driverSettings = MongoDriverSettings.builder()
        .database("demoBoskDB")
        .build();
    return MongoDriver.factory(
        clientSettings,
        driverSettings,
        new BsonPlugin()
    );
}
```

Now you can boot multiple copies of your app and they will share state.
