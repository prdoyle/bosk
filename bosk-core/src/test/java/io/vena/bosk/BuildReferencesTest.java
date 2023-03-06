package io.vena.bosk;

import io.vena.bosk.annotations.ReferencePath;
import io.vena.bosk.exceptions.InvalidTypeException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
	void parameterized() {
		assertEquals(teb.anyEntity(), refs.anyEntity());
	}

	@Test
	void varargs() {
		assertEquals(teb.entityRef(parentID), refs.entity(parentID));
	}

	@Test
	void twoArgs() {
		assertEquals(teb.childrenRef(parentID).then(childID), refs.child(parentID, childID));
	}

	@Test
	void catalogReference() {
		assertEquals(teb.childrenRef(parentID), refs.children(parentID));
	}

	@Test
	void listingReference() throws InvalidTypeException {
		assertEquals(teb.entityRef(parentID).thenListing(TestChild.class, "oddChildren"), refs.oddChildren(parentID));
	}

	@Test
	void sideTableReference() throws InvalidTypeException {
		assertEquals(teb.entityRef(parentID).thenSideTable(TestChild.class, String.class, "stringSideTable"), refs.stringSideTable(parentID));
	}

	@Test
	void returnsNonReference_throws() {
		// We're not all that particular about which exception gets thrown
		assertThrows(InvalidTypeException.class, ()->
			bosk.buildReferences(Invalid_NonReference.class));
	}

	@Test
	void missingAnnotation_throws() {
		assertThrows(InvalidTypeException.class, ()->
			bosk.buildReferences(Invalid_NoAnnotation.class));
	}

	@Test
	void wrongReturnType_throws() {
		assertThrows(InvalidTypeException.class, ()->
			bosk.buildReferences(Invalid_WrongType.class));
	}

	@Test
	void unexpectedParameterType_throws() {
		assertThrows(InvalidTypeException.class, ()->
			bosk.buildReferences(Invalid_WeirdParameter.class));
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

	public interface Invalid_NonReference {
		@ReferencePath("/entities/-entity-")
		String anyEntity();
	}

	public interface Invalid_NoAnnotation {
		Reference<TestEntity> anyEntity();
	}

	public interface Invalid_WrongType {
		@ReferencePath("/entities/-entity-")
		Reference<TestChild> anyEntity();
	}

	public interface Invalid_WeirdParameter {
		@ReferencePath("/entities/-entity-")
		Reference<TestChild> anyEntity(Object parameter);
	}

}
