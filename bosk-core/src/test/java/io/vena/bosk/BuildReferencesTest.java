package io.vena.bosk;

import io.vena.bosk.annotations.ReferencePath;
import io.vena.bosk.exceptions.InvalidTypeException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BuildReferencesTest extends AbstractBoskTest {
	static Bosk<TestRoot> bosk;
	static TestEntityBuilder teb;
	static Refs refs;

	static final Identifier parentID = Identifier.from("parent");
	static final Identifier childID = Identifier.from("child");

	@BeforeAll
	static void setup() throws InvalidTypeException {
		bosk = setUpBosk(Bosk::simpleDriver);
		refs = bosk.buildReferences(Refs.class);

		teb = new TestEntityBuilder(bosk);
	}

	@Test
	void root() {
		assertEquals(bosk.rootReference(), refs.root());
	}

	@Test
	void parameterized() throws InvalidTypeException {
		assertEquals(bosk.reference(TestEntity.class, Path.parseParameterized("/entities/-entity-")),
			refs.anyEntity());
	}

	@Test
	void varargs() throws InvalidTypeException {
		assertEquals(bosk.reference(TestEntity.class, Path.parseParameterized("/entities/-entity-")).boundTo(parentID),
			refs.entity(parentID));
	}

	@Test
	void twoArgs() throws InvalidTypeException {
		assertEquals(bosk.reference(TestChild.class, Path.parseParameterized("/entities/-entity-/children/-child-")).boundTo(parentID, childID),
			refs.child(parentID, childID));
	}

	@Test
	void catalogReference() throws InvalidTypeException {
		assertEquals(bosk.catalogReference(TestChild.class, Path.parseParameterized("/entities/-entity-/children")).boundTo(parentID),
			refs.children(parentID));
	}

	@Test
	void listingReference() throws InvalidTypeException {
		assertEquals(bosk.listingReference(TestEntity.class, Path.parseParameterized("/entities/-entity-/oddChildren")).boundTo(parentID),
			refs.oddChildren(parentID));
	}

	@Test
	void sideTableReference() throws InvalidTypeException {
		assertEquals(bosk.sideTableReference(TestChild.class, String.class, Path.parseParameterized("/entities/-entity-/stringSideTable")).boundTo(parentID),
			refs.stringSideTable(parentID));
	}

	public interface Refs {
		@ReferencePath("/")
		Reference<TestRoot> root();

		@ReferencePath("/entities/-entity-")
		Reference<TestEntity> anyEntity();

		@ReferencePath("/entities/-entity-")
		Reference<TestEntity> entity(Identifier... ids);

		@ReferencePath("/entities/-entity-/children/-child-")
		Reference<TestChild> child(Identifier entity, Identifier child);

		@ReferencePath("/entities/-entity-/children")
		CatalogReference<TestChild> children(Identifier entity);

		@ReferencePath("/entities/-entity-/oddChildren")
		ListingReference<TestChild> oddChildren(Identifier entity);

		@ReferencePath("/entities/-entity-/stringSideTable")
		SideTableReference<TestChild,String> stringSideTable(Identifier parentID);
	}

}
