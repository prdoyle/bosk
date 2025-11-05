package works.bosk.bosonSerializer;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;
import works.bosk.BoskInfo;
import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.Entity;
import works.bosk.Identifier;
import works.bosk.ListValue;
import works.bosk.Listing;
import works.bosk.ListingEntry;
import works.bosk.ListingReference;
import works.bosk.MapValue;
import works.bosk.Path;
import works.bosk.Phantom;
import works.bosk.Reference;
import works.bosk.ReferenceUtils;
import works.bosk.SideTable;
import works.bosk.SideTableReference;
import works.bosk.StateTreeNode;
import works.bosk.StateTreeSerializer;
import works.bosk.TaggedUnion;
import works.bosk.VariantCase;
import works.bosk.boson.mapping.TypeScanner;
import works.bosk.boson.mapping.TypeScanner.Directive;
import works.bosk.boson.mapping.spec.BooleanNode;
import works.bosk.boson.mapping.spec.ComputedSpec;
import works.bosk.boson.mapping.spec.FixedMapMember;
import works.bosk.boson.mapping.spec.FixedMapNode;
import works.bosk.boson.mapping.spec.MaybeAbsentSpec;
import works.bosk.boson.mapping.spec.RepresentAsSpec;
import works.bosk.boson.mapping.spec.StringNode;
import works.bosk.boson.mapping.spec.TypeRefNode;
import works.bosk.boson.mapping.spec.handles.MemberPresenceCondition;
import works.bosk.boson.mapping.spec.handles.TypedHandles;
import works.bosk.boson.types.BoundType;
import works.bosk.boson.types.DataType;
import works.bosk.boson.types.KnownType;
import works.bosk.boson.types.TypeReference;
import works.bosk.exceptions.InvalidTypeException;

import static java.lang.invoke.MethodType.methodType;
import static works.bosk.ListingEntry.LISTING_ENTRY;
import static works.bosk.boson.mapping.spec.handles.MemberPresenceCondition.memberValue;

public class BosonSerializer extends StateTreeSerializer {

	public <
		// Some type variables to use in directives
		T,
		E extends Entity,
		V extends VariantCase
	> TypeScanner.Bundle bundleFor(BoskInfo<?> bosk) {
		MethodHandles.Lookup lookup = MethodHandles.lookup();

		var directives = new ArrayList<Directive>();

		// References are a bit repetitive because we have four
		// different kinds, and they're all very similar.

		directives.add(new Directive(
			DataType.of(new TypeReference<CatalogReference<E>>(){}),
			_ -> RepresentAsSpec.of(
				new RepresentAsSpec.Wrangler<CatalogReference<E>,String>() {
					@Override
					public String toRepresentation(CatalogReference<E> ref) {
						return ref.path().urlEncoded();
					}

					@Override
					@SuppressWarnings("unchecked")
					public CatalogReference<E> fromRepresentation(String str) {
						try {
							return (CatalogReference<E>) bosk.rootReference().thenCatalog(Entity.class, Path.parse(str));
						} catch (InvalidTypeException e) {
							throw new IllegalArgumentException("Failed to parse Reference path: " + str, e);
						}
					}
				}
			)
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<ListingReference<E>>(){}),
			_ -> RepresentAsSpec.of(
				new RepresentAsSpec.Wrangler<ListingReference<E>,String>() {
					@Override
					public String toRepresentation(ListingReference<E> ref) {
						return ref.path().urlEncoded();
					}

					@Override
					@SuppressWarnings("unchecked")
					public ListingReference<E> fromRepresentation(String str) {
						try {
							return (ListingReference<E>) bosk.rootReference().thenListing(Entity.class, Path.parse(str));
						} catch (InvalidTypeException e) {
							throw new IllegalArgumentException("Failed to parse Reference path: " + str, e);
						}
					}
				}
			)
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<SideTableReference<E,T>>(){}),
			_ -> RepresentAsSpec.of(
				new RepresentAsSpec.Wrangler<SideTableReference<E,T>,String>() {
					@Override
					public String toRepresentation(SideTableReference<E,T> ref) {
						return ref.path().urlEncoded();
					}

					@Override
					@SuppressWarnings("unchecked")
					public SideTableReference<E,T> fromRepresentation(String str) {
						try {
							return (SideTableReference<E,T>) bosk.rootReference().thenSideTable(Entity.class, Object.class, Path.parse(str));
						} catch (InvalidTypeException e) {
							throw new IllegalArgumentException("Failed to parse Reference path: " + str, e);
						}
					}
				}
			)
		));

		// If it's not one of the other kinds of Reference, it's a plain Reference<E>
		directives.add(new Directive(
			DataType.of(new TypeReference<Reference<T>>(){}),
			_ -> RepresentAsSpec.of(
				new RepresentAsSpec.Wrangler<Reference<T>,String>() {
					@Override
					public String toRepresentation(Reference<T> ref) {
						return ref.path().urlEncoded();
					}

					@Override
					@SuppressWarnings("unchecked")
					public Reference<T> fromRepresentation(String str) {
						try {
							return (Reference<T>) bosk.rootReference().then(Object.class, Path.parse(str));
						} catch (InvalidTypeException e) {
							throw new IllegalArgumentException("Failed to parse Reference path: " + str, e);
						}
					}
				}
			)
		));

		KnownType idType = DataType.known(Identifier.class);
		directives.add(new Directive(
			idType,
			_ -> RepresentAsSpec.as(
				new StringNode(),
				idType,
				Identifier::toString,
				Identifier::from
			)
		));

		// TODO: I wonder why this is necessary? When do we ever actually encounter a ListingEntry?
		// I get stack overflows in SpecInterpretingGenerator without it.
		DataType listingEntryType = DataType.of(ListingEntry.class);
		directives.add(new Directive(
			listingEntryType,
			_ -> RepresentAsSpec.as(
				new BooleanNode(),
				listingEntryType,
				(ListingEntry _) -> true,
				(Boolean _) -> LISTING_ENTRY
			)
		));

		// TODO: Is this really a bosk thing? Should this be built into boson?
		directives.add(new Directive(
			DataType.CHAR,
			_ -> RepresentAsSpec.as(
				new StringNode(),
				DataType.CHAR,
				Object::toString,
				BosonSerializer::stringToChar
			)
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<Catalog<E>>(){}),
			_ -> RepresentAsSpec.of(new RepresentAsSpec.Wrangler<Catalog<E>, Map<Identifier, E>>() {
				@Override
				public Map<Identifier, E> toRepresentation(Catalog<E> value) {
					return value.asMap();
				}

				@Override
				public Catalog<E> fromRepresentation(Map<Identifier, E> representation) {
					// TODO: validate ids?
					return Catalog.of(representation.values());
				}
			})
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<Listing<E>>(){}),
			_ -> FixedMapNode.of(new FixedMapNode.Wrangler2<CatalogReference<E>, List<Identifier>, Listing<E>>() {
				@Override
				public CatalogReference<E> accessor1(Listing<E> value) {
					return value.domain();
				}

				@Override
				public List<Identifier> accessor2(Listing<E> value) {
					return List.copyOf(value.ids());
				}

				@Override
				public Listing<E> finish(CatalogReference<E> arg1, List<Identifier> arg2) {
					return Listing.of(arg1, arg2);
				}
			})
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<SideTable<E,T>>() { }),
			_ -> RepresentAsSpec.of(new RepresentAsSpec.Wrangler<SideTable<E, T>, SideTableRepresentation<E, T>>() {
				@Override
				public SideTableRepresentation<E, T> toRepresentation(SideTable<E, T> value) {
					return SideTableRepresentation.fromSideTable(value);
				}

				@Override
				public SideTable<E, T> fromRepresentation(SideTableRepresentation<E, T> representation) {
					return representation.toSideTable();
				}
			})
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<TaggedUnion<V>>(){}),
			taggedUnionType -> switch (taggedUnionType) {
				case BoundType bt -> {
					var caseStaticType = (KnownType) bt.parameterType(TaggedUnion.class, 0);
					MapValue<Type> variantCaseMap;
					try {
						variantCaseMap = StateTreeSerializer.getVariantCaseMap(caseStaticType.rawClass());
					} catch (InvalidTypeException e) {
						throw new IllegalArgumentException(e);
					}
					SequencedMap<String, FixedMapMember> members = new LinkedHashMap<>();
					variantCaseMap.forEach((name, caseType) -> {
						var ifPresent = new TypeRefNode(DataType.of(caseType));
						var ifAbsent = new ComputedSpec(TypedHandles.supplier(
							DataType.known(caseType),
							() -> null)); // This is a signal to the finisher that the case is absent
						var presenceCondition = MemberPresenceCondition.enclosingObject(
							TypedHandles.<TaggedUnion<V>, Boolean>function(
								taggedUnionType,
								DataType.BOOLEAN,
								tu -> name.equals(tu.variant().tag())));
						var accessor = TypedHandles.<TaggedUnion<V>, Object>function(
							taggedUnionType,
							DataType.known(caseType),
							TaggedUnion::variant);
						members.put(name, new FixedMapMember(
							new MaybeAbsentSpec(
								ifPresent,
								ifAbsent,
								presenceCondition),
							accessor
						));
					});
					yield FixedMapNode.withArrayFinisher(
						taggedUnionType,
						members,
						(Object[] args) -> {
							for (var arg: args) {
								if (arg instanceof VariantCase vc) {
									return TaggedUnion.of(vc);
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
			DataType.of(new TypeReference<ListValue<T>>(){}),
			listValueType -> switch (listValueType) {
				case BoundType bt -> {
					KnownType elementType = (KnownType) bt.parameterType(ListValue.class, 0);
					@SuppressWarnings("unchecked")
					var factory = listValueFactory((Class<? extends ListValue<T>>)listValueType.leastUpperBoundClass());
					Object[] arrayArchetype = (Object[]) Array.newInstance(elementType.rawClass(), 0);
					yield RepresentAsSpec.of(new RepresentAsSpec.Wrangler<ListValue<T>,List<T>>() {
						@Override
						public List<T> toRepresentation(ListValue<T> value) {
							return value; // ListValue is a List
						}

						@Override
						public ListValue<T> fromRepresentation(List<T> representation) {
							return factory.apply(representation.toArray(arrayArchetype));
						}
					});
				}
				default -> throw new IllegalStateException("Unexpected ListValue type: " + listValueType);
			}
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<MapValue<T>>(){}),
			_ -> RepresentAsSpec.of(new RepresentAsSpec.Wrangler<MapValue<T>, Map<String, T>>() {
				@Override
				public Map<String, T> toRepresentation(MapValue<T> value) {
					return value; // MapValue is a Map
				}

				@Override
				public MapValue<T> fromRepresentation(Map<String, T> representation) {
					return MapValue.copyOf(representation);
				}
			})));

		// TODO
		//
		// It makes me slightly uncomfortable that every directive automatically
		// applies to all subtypes. Just because you can define a bidirectional
		// JSON mapping for some type doesn't mean that the same mapping makes sense
		// for all its subtypes.
		//
		// If I can think of an idiom to restrict directives to exact types only,
		// it might be ok. Otherwise, we probably need to stop using DataType.isAssignableFrom
		// for the matching, and use something more strict that only checks whether
		// one type is a valid generic instantiation of another. Then, if you do
		// want to match all subtypes, you can just use a type variable with an upper bound.
		// My gut feeling is, it's uncommon that a bidirectional JSON mapping can actually
		// handle all subtypes, so this really shouldn't be the default behaviour.
		//
		// I very much want to align this with the JLS spec though, so users don't
		// have to learn special Boson-specific matching rules. I don't want to
		// introduce new concepts for users to learn.
		//
		directives.add(new Directive(
			DataType.of(StateTreeNode.class),
			stateTreeNodeType -> switch (stateTreeNodeType) {
				case BoundType bt -> {
					Class<? extends Record> recordClass = bt.rawClass().asSubclass(Record.class);
					SequencedMap<String, FixedMapMember> componentsByName = new LinkedHashMap<>();
					for (var rc : recordClass.getRecordComponents()) {
						// Look for record components requiring special handling
						if (Optional.class.isAssignableFrom(rc.getType())) {
							var valueType = ReferenceUtils.parameterType(rc.getGenericType(), Optional.class, 0);
							var elementType = new TypeRefNode(DataType.known(valueType));
							var ifPresent = RepresentAsSpec.<Optional<?>, Object>as(
								elementType,
								DataType.known(rc.getGenericType()),
								Optional::get,
								Optional::of
							);
							var ifAbsent = new ComputedSpec(TypedHandles.supplier(DataType.known(rc.getGenericType()),
								Optional::empty));
							var presenceCondition = memberValue(TypedHandles.<Optional<?>>predicate(DataType.known(rc.getGenericType()),
								Optional::isPresent));
							componentsByName.put(rc.getName(), new FixedMapMember(
								new MaybeAbsentSpec(ifPresent, ifAbsent, presenceCondition),
								TypedHandles.componentAccessor(rc, lookup)
							));
						} else if (Phantom.class.isAssignableFrom(rc.getType())) {
							componentsByName.put(rc.getName(), new FixedMapMember(
								new ComputedSpec(TypedHandles.supplier(DataType.known(rc.getGenericType()),
									Phantom::empty)),
								TypedHandles.componentAccessor(rc, lookup)
							));
						} else {
							// A simple TypeRefNode will do
							componentsByName.put(rc.getName(), new FixedMapMember(
								new TypeRefNode(DataType.known(rc.getGenericType())),
								TypedHandles.componentAccessor(rc, lookup)
							));
						}
					}
					yield new FixedMapNode(
						componentsByName,
						TypedHandles.canonicalConstructor(recordClass, lookup)
					);
				}
				default -> throw new IllegalStateException("Unexpected StateTreeNode type: " + stateTreeNodeType);
			}
		));

		return new TypeScanner.Bundle(
			List.of(listingEntryType),
			List.of(lookup),
			List.copyOf(directives)
		);
	}

	private static char stringToChar(String s) {
		if (s.length() == 1) {
			return s.charAt(0);
		} else {
			throw new IllegalArgumentException("Expected single-character string, got: " + s);
		}
	}

	record SideTableRepresentation<K extends Entity, V>(CatalogReference<K> domain, Map<Identifier, V> valuesById){
		public static <KK extends Entity, VV> SideTableRepresentation<KK,VV> fromSideTable(SideTable<KK,VV> sideTable) {
			return new SideTableRepresentation<>(
				sideTable.domain(),
				sideTable.asMap()
			);
		}

		public SideTable<K,V> toSideTable() {
			return SideTable.copyOf(domain, this.valuesById());
		}
	}

	private static final MethodHandle LISTING_OF;

	static {
		try {
			LISTING_OF = MethodHandles.lookup().findStatic(Listing.class, "of", methodType(Listing.class, Reference.class, Collection.class));
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
