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
	void test() throws InvalidTypeException {
		assertEquals(bosk.rootReference(), refs.root());
		assertEquals(teb.anyEntity(), refs.anyEntity());
		assertEquals(teb.entityRef(parentID), refs.anyEntity().boundTo(parentID));
		assertEquals(teb.entityRef(parentID), refs.entity(parentID));
		assertEquals(teb.childrenRef(parentID).then(childID), refs.child(parentID, childID));
		assertEquals(teb.childrenRef(parentID), refs.children(parentID));
		assertEquals(teb.entityRef(parentID).thenListing(TestChild.class, "oddChildren"), refs.oddChildren(parentID));
		assertEquals(teb.entityRef(parentID).thenSideTable(TestChild.class, String.class, "stringSideTable"), refs.stringSideTable(parentID));
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
