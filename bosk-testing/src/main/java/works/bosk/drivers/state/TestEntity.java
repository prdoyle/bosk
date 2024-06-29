package works.bosk.drivers.state;

import works.bosk.Catalog;
import works.bosk.Entity;
import works.bosk.Identifier;
import works.bosk.Listing;
import works.bosk.Reference;
import works.bosk.SideTable;
import java.util.Optional;
import lombok.Value;
import lombok.With;
import lombok.experimental.FieldNameConstants;

@Value
@With
@FieldNameConstants
public class TestEntity implements Entity {
	Identifier id;
	String string;
	Catalog<TestEntity> catalog;
	Listing<TestEntity> listing;
	SideTable<TestEntity, TestEntity> sideTable;
	Optional<TestValues> values;

	public static TestEntity empty(Identifier id, Reference<Catalog<TestEntity>> catalogRef) {
		return new TestEntity(id,
			id.toString(),
			Catalog.empty(),
			Listing.empty(catalogRef),
			SideTable.empty(catalogRef),
			Optional.empty());
	}

}
