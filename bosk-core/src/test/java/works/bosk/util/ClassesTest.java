package works.bosk.util;

import org.junit.jupiter.api.Test;
import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.Entity;
import works.bosk.ListValue;
import works.bosk.Listing;
import works.bosk.ListingReference;
import works.bosk.Reference;
import works.bosk.SideTable;
import works.bosk.SideTableReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClassesTest {

	@Test
	void testCatalog() {
		Class<Catalog<Entity>> catalogClass = Classes.catalog(Entity.class);
		assertEquals(Catalog.class, catalogClass);
	}

	@Test
	void testListing() {
		Class<Listing<Entity>> listingClass = Classes.listing(Entity.class);
		assertEquals(Listing.class, listingClass);
	}

	@Test
	void testSideTable() {
		Class<SideTable<Entity, String>> sideTableClass = Classes.sideTable(Entity.class, String.class);
		assertEquals(SideTable.class, sideTableClass);
	}

	@Test
	void testReference() {
		Class<Reference<String>> reference = Classes.reference(String.class);
		assertEquals(Reference.class, reference);
	}

	@Test
	void testCatalogReference() {
		Class<CatalogReference<Entity>> catalogReferenceClass = Classes.catalogReference(Entity.class);
		assertEquals(CatalogReference.class, catalogReferenceClass);
	}

	@Test
	void testListingReference() {
		Class<ListingReference<Entity>> listingReferenceClass = Classes.listingReference(Entity.class);
		assertEquals(ListingReference.class, listingReferenceClass);
	}

	@Test
	void testSideTableReference() {
		Class<SideTableReference<Entity, String>> sideTableReferenceClass = Classes.sideTableReference(Entity.class, String.class);
		assertEquals(SideTableReference.class, sideTableReferenceClass);
	}

	@Test
	void testListValue() {
		Class<ListValue<String>> listValueClass = Classes.listValue(String.class);
		assertEquals(ListValue.class, listValueClass);
	}

}
