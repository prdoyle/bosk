package works.bosk.boson;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import works.bosk.BoskInfo;
import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.Entity;
import works.bosk.Identifier;
import works.bosk.Listing;
import works.bosk.ListingEntry;
import works.bosk.MapValue;
import works.bosk.Path;
import works.bosk.Reference;
import works.bosk.SideTable;
import works.bosk.StateTreeSerializer;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.json.mapping.TypeMap;
import works.bosk.json.mapping.TypeScanner;
import works.bosk.json.mapping.TypeScanner.Directive;
import works.bosk.json.mapping.spec.BooleanNode;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.mapping.spec.RepresentAsSpec;
import works.bosk.json.mapping.spec.StringNode;
import works.bosk.json.mapping.spec.UniformMapNode;
import works.bosk.json.mapping.spec.handles.ObjectAccumulator;
import works.bosk.json.mapping.spec.handles.TypedHandle;
import works.bosk.json.types.DataType;
import works.bosk.json.types.DataType.BoundType;
import works.bosk.json.types.DataType.KnownType;
import works.bosk.json.types.TypeReference;

import static java.lang.invoke.MethodType.methodType;
import static works.bosk.ListingEntry.LISTING_ENTRY;

public class BosonSerializer extends StateTreeSerializer {

	public TypeScanner.Bundle bundleFor(BoskInfo<?> bosk) {
		var directives = new ArrayList<Directive>();

		var identifierSpec = RepresentAsSpec.as(
			new StringNode(),
			DataType.known(Identifier.class),
			Identifier::toString,
			Identifier::from
		);
		directives.add(new Directive(
			DataType.known(Identifier.class),
			_ -> identifierSpec
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<Reference<?>>(){}),
			referenceType -> RepresentAsSpec.as(
				new StringNode(),
				referenceType,
				(Reference<?> ref) -> ref.path().urlEncoded(),
				(String str) -> {
					try {
						return bosk.rootReference().then(Object.class, Path.parse(str));
					} catch (InvalidTypeException e) {
						throw new IllegalStateException("Not yet implemented", e);
					}
				}
			)
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<Catalog<? extends Entity>>(){}),
			catalogType -> switch (catalogType) {
				case BoundType bt -> {
					var entityType = bt.parameterBinding(Catalog.class, 0);
					yield RepresentAsSpec.as(
						preScan(
							// TODO: This won't work. substitute works only on direct type parameters, not recursively.
							DataType.of(new TypeReference<Map<Identifier, Entity>>() {})
								.substitute(Entity.class, entityType)
						),
						catalogType,
						(Catalog<?> c) -> c.asMap(),
						(Map<Identifier, ? extends Entity> map) -> Catalog.of(map.values())
					);
				}
				default -> throw new IllegalStateException("Unexpected Catalog type: " + catalogType);
			}
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<Listing<?>>(){}),
			listingType -> RepresentAsSpec.as(
				preScan(ListingRepresentation.class),
				listingType,
				ListingRepresentation::fromListing,
				ListingRepresentation::toListing
		)));
		
		directives.add(new Directive(
			DataType.of(new TypeReference<SideTable<?,?>>() { }),
			sideTableType -> RepresentAsSpec.as(
				preScan(SideTableRepresentation.class),
				sideTableType,
				SideTableRepresentation::fromSideTable,
				SideTableRepresentation::toSideTable
			)
		));

		directives.add(new Directive(
			DataType.of(ListingEntry.class),
			_ -> RepresentAsSpec.as(
				new BooleanNode(),
				DataType.known(ListingEntry.class),
				(ListingEntry _) -> true,
				(Boolean _) -> LISTING_ENTRY
			)
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<MapValue<?>>(){}),
			mapValueType -> switch (mapValueType) {
				case BoundType bt -> {
					// It's just like a Map, only we want to instantiate MapValue instead of LinkedHashMap
					var mapSpec = (UniformMapNode)preScan(
						DataType.of(new TypeReference<Map<String, Entity>>(){})
							.substitute(Entity.class, bt.parameterBinding(MapValue.class, 1)));
					try {
						yield new UniformMapNode(
							mapSpec.keyNode(),
							mapSpec.valueNode(),
							new ObjectAccumulator(
								mapSpec.accumulator().creator(),
								mapSpec.accumulator().integrator(),
								new TypedHandle( // here's our change
									MethodHandles.lookup().findStatic(MapValue.class, "copyOf", methodType(MapValue.class, Map.class)),
									mapValueType,
									List.of(mapSpec.dataType())
								)
							),
							mapSpec.emitter()
						);
					} catch (NoSuchMethodException | IllegalAccessException e) {
						throw new IllegalStateException("Unexpected problem trying to access MapValue.copyOf", e);
					}
				}
				default -> throw new IllegalStateException("Unexpected MapValue type: " + mapValueType);
			}));

		return new TypeScanner.Bundle(List.copyOf(directives));
	}
	
	private JsonValueSpec preScan(Type type) {
		return preScan(DataType.of(type));
	}

	private JsonValueSpec preScan(TypeReference<?> ref) {
		return preScan(DataType.of(ref));
	}

	private static JsonValueSpec preScan(DataType dataType) {
		var ts = new TypeScanner(TypeMap.Settings.RAW)
			.scan(dataType);
		return ts.build().get(dataType);
	}

	public record ListingRepresentation(CatalogReference<?> domain, List<Identifier> ids){
		public static ListingRepresentation fromListing(Listing<?> listing) {
			return new ListingRepresentation(
				listing.domain(),
				new ArrayList<>(listing.ids())
			);
		}

		@SuppressWarnings("rawtypes")
		public Listing toListing() {
			return Listing.of(domain, this.ids());
		}
	}
	
	public record SideTableRepresentation(CatalogReference<?> domain, Map<Identifier, ?> valuesById){
		public static SideTableRepresentation fromSideTable(SideTable<?,?> sideTable) {	
			return new SideTableRepresentation(
				sideTable.domain(),
				sideTable.asMap()
			);
		}

		@SuppressWarnings("rawtypes")
		public SideTable toSideTable() {
			return SideTable.copyOf(domain, this.valuesById());
		}
	}

	interface Representation<V,R> {
		R to(V value);
		V from(R representation);
	}

	JsonValueSpec representedAs(Representation<?,?> representation, KnownType actualType) {
		BoundType bt = (BoundType) DataType.known(representation.getClass());
		Type valueType = bt
			.parameterBinding(Representation.class, 0);
		assert !(DataType.of(valueType) instanceof KnownType kt)
			|| kt.rawClass().equals(actualType.rawClass());
		Type repType = bt
			.parameterBinding(Representation.class, 1);
		KnownType rt = DataType.known(repType);
		try {
			return new RepresentAsSpec(
				preScan(repType),
				new TypedHandle(
					MethodHandles.lookup().findVirtual(representation.getClass(), "to", methodType(Object.class, Object.class))
						.bindTo(representation)
						.asType(methodType(rt.rawClass(), actualType.rawClass())),
					rt,
					List.of(actualType)
				),
				new TypedHandle(
					MethodHandles.lookup().findVirtual(representation.getClass(), "from", methodType(Object.class, Object.class))
						.bindTo(representation)
						.asType(methodType(actualType.rawClass(), rt.rawClass())),
					actualType,
					List.of(rt)
			));
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new IllegalStateException("wat", e);
		}
	}
}
