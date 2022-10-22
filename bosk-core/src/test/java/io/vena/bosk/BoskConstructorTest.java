package io.vena.bosk;

import io.vena.bosk.Bosk.DefaultRootFunction;
import io.vena.bosk.TypeValidationTest.MutableField;
import io.vena.bosk.TypeValidationTest.Primitives;
import io.vena.bosk.TypeValidationTest.SimpleTypes;
import io.vena.bosk.drivers.ForwardingDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static io.vena.bosk.TypeValidationTest.SimpleTypes.MyEnum.LEFT;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * These tests don't use @{@link org.junit.jupiter.api.BeforeEach}
 * to pre-create a {@link Bosk} because we want to test the constructor itself.
 */
public class BoskConstructorTest {

	@Test
	void basicProperties_correctValues() {
		String name = "Name";
		Type rootType = SimpleTypes.class;
		Entity root = newEntity();

		AtomicReference<BoskDriver<Entity>> driver = new AtomicReference<>();
		Bosk<Entity> bosk = new Bosk<Entity>(
			name,
			rootType,
			__ -> root,
			(r,d)-> {
				driver.set(new ForwardingDriver<>(singleton(d)));
				return driver.get();
			});

		assertEquals(name, bosk.name());
		assertEquals(rootType, bosk.rootReference().targetType());

		// The driver object and root node should be exactly the same object passed in

		assertSame(driver.get(), bosk.driver());

		try (val __ = bosk.readContext()) {
			assertSame(root, bosk.rootReference().value());
		}
	}

	/**
	 * Not a thorough test of type validation. Just testing that invalid types are rejected.
	 *
	 * @see TypeValidationTest
	 */
	@Test
	void invalidRootType_throws() {
		assertThrows(IllegalArgumentException.class, ()->
			new Bosk<MutableField>(
				"Invalid root type",
				MutableField.class,
				bosk -> new MutableField(),
				Bosk::simpleDriver));
	}

	@Test
	void badDriverInitialRoot_throws() {
		assertInitialRootThrows(NullPointerException.class, () -> null);
		assertInitialRootThrows(ClassCastException.class, () -> new TypeValidationTest.CatalogOfInvalidType(Identifier.from("whoops"), Catalog.empty()));
		assertInitialRootThrows(IllegalArgumentException.class, () -> { throw new InvalidTypeException("Whoopsie"); });
		assertInitialRootThrows(IllegalArgumentException.class, () -> { throw new IOException("Whoopsie"); });
		assertInitialRootThrows(IllegalArgumentException.class, () -> { throw new InterruptedException("Whoopsie"); });
	}

	@Test
	void badDefaultRootFunction_throws() {
		assertDefaultRootThrows(NullPointerException.class, __ -> null);
		assertDefaultRootThrows(ClassCastException.class, __ -> new TypeValidationTest.CatalogOfInvalidType(Identifier.from("whoops"), Catalog.empty()));
		assertDefaultRootThrows(IllegalArgumentException.class, __ -> { throw new InvalidTypeException("Whoopsie"); });
	}

	@Test
	void mismatchedRootType_throws() {
		assertThrows(ClassCastException.class, ()->
			new Bosk<Entity> (
				"Mismatched root",
				Primitives.class, // Valid but wrong
				bosk -> newEntity(),
				Bosk::simpleDriver
			)
		);
	}

	@Test
	void driverInitialRoot_matches() {
		SimpleTypes root = newEntity();
		Bosk<Entity> bosk = new Bosk<Entity>(
			"By value",
			SimpleTypes.class,
			__ -> {throw new AssertionError("Shouldn't be called");},
			initialRootDriver(()->root));
		try (val __ = bosk.readContext()) {
			assertSame(root, bosk.rootReference().value());
		}
	}

	@Test
	void defaultRoot_matches() {
		SimpleTypes root = newEntity();
		{
			Bosk<Entity> valueBosk = new Bosk<>("By value", SimpleTypes.class, root, Bosk::simpleDriver);
			try (val __ = valueBosk.readContext()) {
				assertSame(root, valueBosk.rootReference().value());
			}
		}

		{
			Bosk<Entity> functionBosk = new Bosk<Entity>("By value", SimpleTypes.class, __ -> root, Bosk::simpleDriver);
			try (val __ = functionBosk.readContext()) {
				assertSame(root, functionBosk.rootReference().value());
			}
		}
	}

	////////////////
	//
	//  Helpers
	//

	private static void assertInitialRootThrows(Class<? extends Throwable> expectedType, InitialRootFunction initialRootFunction) {
		assertThrows(expectedType, () -> new Bosk<>(
			"Throw test",
			SimpleTypes.class,
			newEntity(),
			initialRootDriver(initialRootFunction)
		));
	}

	private static void assertDefaultRootThrows(Class<? extends Throwable> expectedType, DefaultRootFunction<Entity> defaultRootFunction) {
		assertThrows(expectedType, () -> new Bosk<>(
			"Throw test",
			SimpleTypes.class,
			defaultRootFunction,
			Bosk::simpleDriver
		));
	}

	@NotNull
	private static DriverFactory<Entity> initialRootDriver(InitialRootFunction initialRootFunction) {
		return (r,d) -> new ForwardingDriver<Entity>(emptyList()) {
			@Override
			public Entity initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
				return initialRootFunction.get();
			}
		};
	}

	interface InitialRootFunction {
		Entity get() throws InvalidTypeException, IOException, InterruptedException;
	}

	private static SimpleTypes newEntity() {
		return new SimpleTypes(Identifier.unique("test"), "string", LEFT);
	}

}
