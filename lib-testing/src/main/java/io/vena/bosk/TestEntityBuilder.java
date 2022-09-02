package io.vena.bosk;

import io.vena.bosk.exceptions.InvalidTypeException;

public class TestEntityBuilder {
	private final Reference<AbstractBoskTest.TestEntity> anyEntity;
	private final CatalogReference<AbstractBoskTest.TestChild> anyChildren;
	private final Reference<AbstractBoskTest.ImplicitRefs> anyImplicitRefs;

	public TestEntityBuilder(Bosk<AbstractBoskTest.TestRoot> bosk) throws InvalidTypeException {
		this.anyEntity = bosk.rootReference().then(AbstractBoskTest.TestEntity.class, AbstractBoskTest.TestRoot.Fields.entities, "-entity-");
		this.anyChildren = anyEntity.thenCatalog(AbstractBoskTest.TestChild.class, AbstractBoskTest.TestEntity.Fields.children);
		this.anyImplicitRefs = anyEntity.then(AbstractBoskTest.ImplicitRefs.class, AbstractBoskTest.TestEntity.Fields.implicitRefs);
	}

	public Reference<AbstractBoskTest.TestEntity> entityRef(Identifier id) { return anyEntity.boundTo(id); }
	public CatalogReference<AbstractBoskTest.TestChild> childrenRef(Identifier entityID) { return anyChildren.boundTo(entityID); }
	public Reference<AbstractBoskTest.ImplicitRefs> implicitRefsRef(Identifier entityID) { return anyImplicitRefs.boundTo(entityID); }

	public AbstractBoskTest.TestEntity blankEntity(Identifier id, AbstractBoskTest.TestEnum testEnum) {
		return new AbstractBoskTest.TestEntity(id,
			id.toString(),
			testEnum,
			Catalog.empty(),
			Listing.empty(childrenRef(id)),
			SideTable.empty(childrenRef(id)),
			AbstractBoskTest.Phantoms.empty(Identifier.from(id + "_phantoms")),
			AbstractBoskTest.Optionals.empty(Identifier.from(id + "_optionals")),
			new AbstractBoskTest.ImplicitRefs(Identifier.from(id + "_implicitRefs"),
				implicitRefsRef(id), entityRef(id),
				implicitRefsRef(id), entityRef(id)));
	}

}
