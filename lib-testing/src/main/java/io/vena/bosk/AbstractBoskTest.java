package io.vena.bosk;

import org.pcollections.OrderedPMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.vena.bosk.annotations.Enclosing;
import io.vena.bosk.annotations.Self;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.With;
import lombok.experimental.Accessors;
import lombok.experimental.FieldNameConstants;
import org.pcollections.OrderedPSet;
import org.pcollections.PBag;
import org.pcollections.PCollection;
import org.pcollections.PMap;
import org.pcollections.PQueue;
import org.pcollections.PSequence;
import org.pcollections.PSet;
import org.pcollections.PSortedMap;
import org.pcollections.PSortedSet;
import org.pcollections.PStack;
import org.pcollections.PVector;
import org.pcollections.TreePMap;
import org.pcollections.TreePSet;
import org.pcollections.TreePVector;

import static java.util.Arrays.asList;

public abstract class AbstractBoskTest {
	@Value
	@Accessors(fluent=true)
	@With
	@FieldNameConstants
	public static class TestRoot implements Entity {
		Identifier id;
		Catalog<TestEntity> entities;
		StringListValueSubclass someStrings;
		MapValue<String> someMappedStrings;
	}

	public static class StringListValueSubclass extends ListValue<String> {
		public StringListValueSubclass(String... entries) {
			super(entries);
		}
	}

	@Value
	@Accessors(fluent=true)
	@With
	@FieldNameConstants
	public static class TestEntity implements Entity {
		Identifier id;
		String string;
		TestEnum testEnum;
		Catalog<TestChild> children;
		Listing<TestChild> oddChildren;
		SideTable<TestChild,String> stringSideTable;
		Phantoms phantoms;
		Optionals optionals;
		ImplicitRefs implicitRefs;
		PCollections pCollections;

		public TestEntity withChild(TestChild child) {
			return this.withChildren(children.with(child));
		}
	}

	@Value
	@Accessors(fluent=true)
	@With
	@FieldNameConstants
	public static class TestChild implements Entity {
		Identifier id;
		String string;
		TestEnum testEnum;
		Catalog<TestChild> recursiveChildren;
	}

	@Value
	@Accessors(fluent=true)
	@With
	@FieldNameConstants
	public static class Optionals implements Entity {
		Identifier id;
		Optional<String> optionalString;
		Optional<TestChild> optionalEntity;
		Optional<Reference<TestEntity>> optionalRef;
		Optional<Catalog<TestChild>> optionalCatalog;
		Optional<Listing<TestChild>> optionalListing;
		Optional<SideTable<TestChild,String>> optionalSideTable;

		public static Optionals empty(Identifier id) {
			return new Optionals(id,
				Optional.empty(), Optional.empty(), Optional.empty(),
				Optional.empty(), Optional.empty(), Optional.empty());
		}
	}

	@Value
	@Accessors(fluent=true)
	@With
	@FieldNameConstants
	public static class Phantoms implements Entity {
		Identifier id;
		Phantom<String> phantomString;
		Phantom<TestChild> phantomEntity;
		Phantom<Reference<TestEntity>> phantomRef;
		Phantom<Catalog<TestChild>> phantomCatalog;
		Phantom<Listing<TestChild>> phantomListing;
		Phantom<SideTable<TestChild,String>> phantomSideTable;

		public static Phantoms empty(Identifier id) {
			return new Phantoms(id,
				Phantom.empty(), Phantom.empty(), Phantom.empty(),
				Phantom.empty(), Phantom.empty(), Phantom.empty());
		}
	}

	@Value
	@EqualsAndHashCode(callSuper=true)
	@Accessors(fluent=true)
	@With
	@FieldNameConstants
	public static class ImplicitRefs extends ReflectiveEntity<ImplicitRefs> {
		Identifier id;
		Reference<ImplicitRefs> reference;
		Reference<TestEntity> enclosingRef;
		@Self Reference<ImplicitRefs> reference2;
		@Enclosing Reference<TestEntity> enclosingRef2;

		public ImplicitRefs(Identifier id, @Self Reference<ImplicitRefs> reference, @Enclosing Reference<TestEntity> enclosingRef, Reference<ImplicitRefs> reference2, Reference<TestEntity> enclosingRef2) {
			this.id = id;
			this.reference = reference;
			this.enclosingRef = enclosingRef;
			this.reference2 = reference2;
			this.enclosingRef2 = enclosingRef2;
		}
	}

	/**
	 * We support pcollections that have a defined order.
	 */
	@Value
	@Accessors(fluent=true)
	@With
	@FieldNameConstants
	public static class PCollections implements Entity {
		Identifier id;
//		PSequence<String> pSequence;
//		PSortedSet<String> pSortedSet;
//		PSortedMap<String, String> pSortedMap;
		OrderedPSet<String> orderedPSet;
		OrderedPMap<String, String> orderedPMap;
	}

	public enum TestEnum {
		OK,
		NOT_SO_OK
	}

	protected static abstract class AbstractReference<T> implements Reference<T> {
		@Override public Path path() { return null; }
		@Override public Type targetType() { return null; }
		@Override public Class<T> targetClass() { return null; }
		@Override public T valueIfExists() { return null; }
		@Override public void forEachValue(BiConsumer<T, BindingEnvironment> action, BindingEnvironment existingEnvironment) { }

		@Override public <U> Reference<U> then(Class<U> targetClass, String... segments) { return null; }
		@Override public <E extends Entity> CatalogReference<E> thenCatalog(Class<E> entryClass, String... segments) { return null; }
		@Override public <E extends Entity> ListingReference<E> thenListing(Class<E> entryClass, String... segments) { return null; }
		@Override public <K extends Entity, V> SideTableReference<K, V> thenSideTable(Class<K> keyClass, Class<V> valueClass, String... segments) { return null; }
		@Override public <TT> Reference<Reference<TT>> thenReference(Class<TT> targetClass, String... segments) { return null; }
		@Override public <TT> Reference<TT> enclosingReference(Class<TT> targetClass) { return null; }
		@Override public Reference<T> boundBy(BindingEnvironment bindings) { return null; }

	}

	protected static Bosk<TestRoot> setUpBosk(DriverFactory<TestRoot> driverFactory) {
		return new Bosk<TestRoot>("Test", TestRoot.class, AbstractRoundTripTest::initialRoot, driverFactory);
	}

	protected static TestRoot initialRoot(Bosk<TestRoot> bosk) {
		TestEntityBuilder teb;
		try {
			teb = new TestEntityBuilder(bosk);
		} catch (InvalidTypeException e) {
			throw new NotYetImplementedException(e);
		}
		Identifier parentID = Identifier.from("parent");
		Reference<TestEntity> parentRef = teb.entityRef(parentID);
		CatalogReference<TestChild> childrenRef = teb.childrenRef(parentID);
		Identifier child1ID = Identifier.from("child1");
		Identifier child2ID = Identifier.from("child2");
		Identifier child3ID = Identifier.from("child3");
		TestEntity entity = new TestEntity(parentID, "parent", TestEnum.OK, Catalog.of(
			new TestChild(child1ID, "child1", TestEnum.OK, Catalog.empty()),
			new TestChild(child2ID, "child2", TestEnum.NOT_SO_OK, Catalog.empty()),
			new TestChild(child3ID, "child3", TestEnum.OK, Catalog.empty())
		),
			Listing.empty(childrenRef).withID(child1ID).withID(child3ID),
			SideTable.empty(childrenRef, String.class).with(child2ID, "I'm child 2"),
			Phantoms.empty(Identifier.from("phantoms")),
			new Optionals(Identifier.from("optionals"),
				Optional.of("rootString"),
				Optional.of(new TestChild(Identifier.from("entity2"), "entity2", TestEnum.OK, Catalog.empty())),
				Optional.of(parentRef),
				Optional.of(Catalog.of(new TestChild(Identifier.from("OptionalTestEntity2"), "OptionalTestEntity2", TestEnum.OK, Catalog.empty()))),
				Optional.of(Listing.of(childrenRef, child2ID)),
				Optional.of(SideTable.empty(childrenRef, String.class).with(child2ID, "String value associated with " + child2ID))
			),
			new ImplicitRefs(Identifier.from("parent_implicitRefs"),
				teb.implicitRefsRef(parentID), parentRef,
				teb.implicitRefsRef(parentID), parentRef),
			new PCollections(Identifier.from("pcollections"),
				// Note that alphabetical and numerical order differ in these test values
//				TreePVector.singleton("one").plus("two").plus("three"),
//				TreePSet.singleton("one").plus("two").plus("three"),
//				TreePMap.singleton("oneK", "oneV").plus("twoK","twoV").plus("threeK","threeV"),
				OrderedPSet.singleton("one").plus("two").plus("three"),
				OrderedPMap.singleton("oneK", "oneV").plus("twoK","twoV").plus("threeK","threeV")
				));
		return new TestRoot(
			Identifier.from("root"),
			Catalog.of(entity),
			new StringListValueSubclass("One", "Two"),
			MapValue.fromFunction(asList("key1", "key2"), k ->k + "_value"));
	}

	protected Gson gsonFor(Bosk<TestRoot> bosk) {
		return new GsonBuilder()
			.setPrettyPrinting()
			.registerTypeAdapterFactory(new GsonPlugin().adaptersFor(bosk))
			.create();
	}

	protected Reference<TestEntity> entityReference(Identifier id, Bosk<TestRoot> bosk) {
		try {
			return bosk.catalogReference(TestEntity.class, Path.just(TestRoot.Fields.entities)).then(id);
		} catch (InvalidTypeException e) {
			throw new AssertionError("Expected entities reference to work", e);
		}
	}
}
