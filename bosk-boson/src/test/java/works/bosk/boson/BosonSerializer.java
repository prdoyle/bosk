package works.bosk.boson;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;
import java.util.function.Function;
import works.bosk.BoskInfo;
import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.Entity;
import works.bosk.Identifier;
import works.bosk.Listing;
import works.bosk.ListingEntry;
import works.bosk.MapValue;
import works.bosk.Path;
import works.bosk.Phantom;
import works.bosk.Reference;
import works.bosk.SideTable;
import works.bosk.StateTreeNode;
import works.bosk.StateTreeSerializer;
import works.bosk.TaggedUnion;
import works.bosk.VariantCase;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.json.mapping.TypeMap;
import works.bosk.json.mapping.TypeScanner;
import works.bosk.json.mapping.TypeScanner.Directive;
import works.bosk.json.mapping.spec.BooleanNode;
import works.bosk.json.mapping.spec.ComputedSpec;
import works.bosk.json.mapping.spec.FixedMapMember;
import works.bosk.json.mapping.spec.FixedMapNode;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.mapping.spec.MaybeAbsentSpec;
import works.bosk.json.mapping.spec.RepresentAsSpec;
import works.bosk.json.mapping.spec.StringNode;
import works.bosk.json.mapping.spec.UniformMapNode;
import works.bosk.json.mapping.spec.handles.TypedHandle;
import works.bosk.json.types.BoundType;
import works.bosk.json.types.DataType;
import works.bosk.json.types.DeferredParameterOrBound;
import works.bosk.json.types.KnownType;
import works.bosk.json.types.ParameterOrBound;
import works.bosk.json.types.SpecifiedParameterOrBound;
import works.bosk.json.types.TypeReference;
import works.bosk.json.types.UpperBoundedWildcardType;

import static works.bosk.ListingEntry.LISTING_ENTRY;
import static works.bosk.json.mapping.spec.handles.MemberPresenceCondition.memberValue;

public class BosonSerializer extends StateTreeSerializer {

	public <V> TypeScanner.Bundle bundleFor(BoskInfo<?> bosk) {
		var directives = new ArrayList<Directive>();

		directives.add(new Directive(
			DataType.of(new TypeReference<Catalog<? extends Entity>>(){}),
			catalogType -> switch (catalogType) {
				case BoundType bt -> {
					JsonValueSpec representation = mapWithIdentifierKeys(
						bt.parameterBinding(Catalog.class, 0));
					yield RepresentAsSpec.as(
						representation,
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
			DataType.known(Identifier.class),
			identifierType -> RepresentAsSpec.as(
				new StringNode(),
				identifierType,
				Identifier::toString,
				Identifier::from
			)
		));

		directives.add(new Directive(
			DataType.of(ListingEntry.class),
			listingEntryType -> RepresentAsSpec.as(
				new BooleanNode(),
				listingEntryType,
				(ListingEntry _) -> true,
				(Boolean _) -> LISTING_ENTRY
			)
		));

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
			DataType.of(new TypeReference<TaggedUnion<?>>(){}),
			taggedUnionType -> switch (taggedUnionType) {
				case BoundType bt -> {
					var caseStaticType = (KnownType)bt.parameterType(TaggedUnion.class, 0);
					MapValue<Type> variantCaseMap;
					try {
						variantCaseMap = StateTreeSerializer.getVariantCaseMap(caseStaticType.rawClass());
					} catch (InvalidTypeException e) {
						throw new IllegalArgumentException(e);
					}
					SequencedMap<String, FixedMapMember> members = new LinkedHashMap<>();
					variantCaseMap.forEach((name, caseType) -> {
						members.put(name, new FixedMapMember(
							preScan(caseType),
							TypedHandle.ofFunction((Function<TaggedUnion<?>, Object>) TaggedUnion::variant)
						));
					});
					yield FixedMapNode.withArrayFinisher(
						members,
						(VariantCase[] args) -> {
							for (var arg: args) {
								if (arg != null) {
									return TaggedUnion.of(arg);
								}
							}
							throw new IllegalStateException("Hey, no variant");
						}
					);
				}
				default -> throw new IllegalStateException("Unexpected value: " + taggedUnionType);
			}
		));

		directives.add(new Directive(
			new UpperBoundedWildcardType(new DeferredParameterOrBound(StateTreeNode.class)),
			stateTreeNodeType -> switch (stateTreeNodeType) {
				case BoundType bt -> {
					// Configure the preScan so it does the right thing with special components
					Class<? extends Record> recordClass = bt.rawClass().asSubclass(Record.class);
					Map<String, FixedMapMember> componentsByName = new HashMap<>();
					for (var rc: recordClass.getRecordComponents()) {
						if (Optional.class.isAssignableFrom(rc.getType())) {
							var ifPresent = preScan(rc.getGenericType());
							var ifAbsent = new ComputedSpec(TypedHandle.ofSupplier(Optional::empty));
							var presenceCondition = memberValue(TypedHandle.<Optional<?>>ofPredicate(Optional::isPresent));
							componentsByName.put(rc.getName(), new FixedMapMember(
								new MaybeAbsentSpec(ifPresent, ifAbsent, presenceCondition),
								TypedHandle.ofComponentAccessor(rc)
							));
						} else if (Phantom.class.isAssignableFrom(rc.getType())) {
							componentsByName.put(rc.getName(), new FixedMapMember(
								new ComputedSpec(TypedHandle.ofSupplier(Phantom::empty)),
								TypedHandle.ofComponentAccessor(rc)
							));
						}
					}

					// Now, with this in place, a shallow preScan returns the right thing
					yield preScan(bt,
						new TypeScanner(TypeMap.Settings.SHALLOW)
							.specifyRecordFields(recordClass, componentsByName)
					);
				}
				default -> throw new IllegalStateException("Unexpected StateTreeNode type: " + stateTreeNodeType);
			}
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<MapValue<?>>(){}),
			mapValueType -> switch (mapValueType) {
				case BoundType bt -> {
					var mapSpec = mapWithIdentifierKeys(bt.parameterBinding(MapValue.class, 1));
					yield RepresentAsSpec.as(
						mapSpec,
						mapValueType,
						(MapValue<V> mv) -> mv, // MapValue is a Map
						MapValue::copyOf
					);
				}
				default -> throw new IllegalStateException("Unexpected MapValue type: " + mapValueType);
			}));

		return new TypeScanner.Bundle(List.copyOf(directives));
	}

	private static UniformMapNode mapWithIdentifierKeys(ParameterOrBound e2) {
		return (UniformMapNode)preScan(
			new BoundType(Map.class, List.of(
				new SpecifiedParameterOrBound(DataType.known(Identifier.class)),
				e2
			)));
	}

	private JsonValueSpec preScan(Type type) {
		return preScan(DataType.of(type));
	}

	private JsonValueSpec preScan(TypeReference<?> ref) {
		return preScan(DataType.of(ref));
	}

	private static JsonValueSpec preScan(DataType dataType) {
		return preScan(dataType, new TypeScanner(TypeMap.Settings.SHALLOW));
	}

	private static JsonValueSpec preScan(DataType dataType, TypeScanner typeScanner) {
		return typeScanner.scan(dataType).build().get(dataType);
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

}
