package works.bosk;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import works.bosk.Bosk.DefaultRootFunction;
import works.bosk.TypeValidationTest.BoxedPrimitives;
import works.bosk.TypeValidationTest.MutableField;
import works.bosk.TypeValidationTest.SimpleTypes;
import works.bosk.drivers.ForwardingDriver;
import works.bosk.exceptions.InvalidTypeException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.BoskTestUtils.boskName;
import static works.bosk.TypeValidationTest.SimpleTypes.MyEnum.LEFT;

/**
 * These tests don't use @{@link org.junit.jupiter.api.BeforeEach}
 * to pre-create a {@link Bosk} because we want to test the constructor itself.
 */
public class BoskConstructorTest {

	@Test
	void basicProperties_correctValues() {
		String name = boskName();
		Type rootType = SimpleTypes.class;
		StateTreeNode root = newEntity();

		AtomicReference<BoskDriver<StateTreeNode>> driver = new AtomicReference<>();
		Bosk<StateTreeNode> bosk = new Bosk<StateTreeNode>(
			name,
			rootType,
			_ -> root,
			(b,d)-> {
				driver.set(new ForwardingDriver<>(singleton(d)));
				return driver.get();
			});

		assertEquals(name, bosk.name());
		assertEquals(rootType, bosk.rootReference().targetType());

		// The driver object and root node should be exactly the same object passed in

		assertSame(driver.get(), bosk.getDriver(ForwardingDriver.class));

		try (var _ = bosk.readContext()) {
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
				boskName("Invalid root type"),
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
		assertDefaultRootThrows(NullPointerException.class, _ -> null);
		assertDefaultRootThrows(ClassCastException.class, _ -> new TypeValidationTest.CatalogOfInvalidType(Identifier.from("whoops"), Catalog.empty()));
		assertDefaultRootThrows(IllegalArgumentException.class, _ -> { throw new InvalidTypeException("Whoopsie"); });
	}

	@Test
	void mismatchedRootType_throws() {
		assertThrows(ClassCastException.class, ()->
			new Bosk<Entity> (
				boskName("Mismatched root"),
				BoxedPrimitives.class, // Valid but wrong
				bosk -> newEntity(),
				Bosk::simpleDriver
			)
		);
	}

	@Test
	void driverInitialRoot_matches() {
		SimpleTypes root = newEntity();
		Bosk<StateTreeNode> bosk = new Bosk<StateTreeNode>(
			boskName(),
			SimpleTypes.class,
			_ -> {throw new AssertionError("Shouldn't be called");},
			initialRootDriver(()->root));
		try (var _ = bosk.readContext()) {
			assertSame(root, bosk.rootReference().value());
		}
	}

	@Test
	void defaultRoot_matches() {
		SimpleTypes root = newEntity();
		{
			Bosk<StateTreeNode> valueBosk = new Bosk<>(boskName(), SimpleTypes.class, root, Bosk::simpleDriver);
			try (var _ = valueBosk.readContext()) {
				assertSame(root, valueBosk.rootReference().value());
			}
		}

		{
			Bosk<StateTreeNode> functionBosk = new Bosk<StateTreeNode>(boskName(), SimpleTypes.class, _ -> root, Bosk::simpleDriver);
			try (var _ = functionBosk.readContext()) {
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
			boskName(),
			SimpleTypes.class,
			newEntity(),
			initialRootDriver(initialRootFunction)
		));
	}

	private static void assertDefaultRootThrows(Class<? extends Throwable> expectedType, DefaultRootFunction<StateTreeNode> defaultRootFunction) {
		assertThrows(expectedType, () -> new Bosk<>(
			boskName(),
			SimpleTypes.class,
			defaultRootFunction,
			Bosk::simpleDriver
		));
	}

	@NotNull
	private static DriverFactory<StateTreeNode> initialRootDriver(InitialRootFunction initialRootFunction) {
		return (b,d) -> new ForwardingDriver<StateTreeNode>(emptyList()) {
			@Override
			public StateTreeNode initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
				return initialRootFunction.get();
			}
		};
	}

	interface InitialRootFunction {
		StateTreeNode get() throws InvalidTypeException, IOException, InterruptedException;
	}

	private static SimpleTypes newEntity() {
		return new SimpleTypes(Identifier.unique("test"), "string", LEFT);
	}

}
