package works.bosk.json.mapping;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.json.mapping.opt.Optimizer;
import works.bosk.json.mapping.spec.ArrayNode;
import works.bosk.json.mapping.spec.BigNumberNode;
import works.bosk.json.mapping.spec.BooleanNode;
import works.bosk.json.mapping.spec.BoxedPrimitiveSpec;
import works.bosk.json.mapping.spec.EnumByNameNode;
import works.bosk.json.mapping.spec.FixedMapMember;
import works.bosk.json.mapping.spec.FixedMapNode;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.mapping.spec.MaybeNullSpec;
import works.bosk.json.mapping.spec.PrimitiveNumberNode;
import works.bosk.json.mapping.spec.StringNode;
import works.bosk.json.mapping.spec.StringSpec;
import works.bosk.json.mapping.spec.TypeRefNode;
import works.bosk.json.mapping.spec.UniformMapNode;
import works.bosk.json.mapping.spec.handles.ArrayAccumulator;
import works.bosk.json.mapping.spec.handles.ArrayEmitter;
import works.bosk.json.mapping.spec.handles.ObjectAccumulator;
import works.bosk.json.mapping.spec.handles.ObjectEmitter;
import works.bosk.json.mapping.spec.handles.TypedHandle;
import works.bosk.json.types.DataType;
import works.bosk.json.types.DataType.ArrayType;
import works.bosk.json.types.DataType.InstanceType;
import works.bosk.json.types.DataType.KnownType;
import works.bosk.json.types.DataType.PrimitiveType;

import static java.util.stream.Collectors.toSet;
import static works.bosk.json.mapping.spec.PrimitiveNumberNode.PRIMITIVE_NUMBER_CLASSES;

/**
 * Collects information about {@link DataType}s,
 * and then {@link #build() builds} an optimized {@link TypeMap}.
 */
public class TypeScanner {
	final Map<KnownType, TypeRefNode> refs = new LinkedHashMap<>();
	final TypeMap inProgress;
	final Map<Class<? extends Record>, Map<String, FixedMapMember>> recordComponentOverrides = new HashMap<>();

	public TypeScanner(TypeMap.Settings settings) {
		this.inProgress = new TypeMap(settings);
	}

	/**
	 * Inspects the given {@code type} and deduces the appropriate {@link JsonValueSpec} for it.
	 * Has no effect if the given {@code type} has already been specified,
	 * so this is safe to call just to make sure a given type has been specified.
	 * <p>
	 * Recursively scans types referenced from the given {@code type}.
	 * Generally you'll want to make any calls to {@link #specify} first before calling this,
	 * because you can't override a specification afterward.
	 * (This is no great loss, since recursively scanning a given type only to override
	 * it later would be inefficient anyway.)
	 *
	 * @return this
	 */
	public TypeScanner scan(DataType type) {
		inProgress.computeIfAbsent(type, this::computeSpecNode);
		return this;
	}

	/**
	 * Indicates that the given {@code type} is to be associated with the given {@code spec}.
	 * <p>
	 * <em>Note</em>: this can only specify handling for whole types. If you want to override
	 * handling for one particular record field, or for the keys or values of a particular map,
	 * or even the entries of a particular list, that can't be done here.
	 *
	 * @return this
	 * @throws IllegalArgumentException if {@code type} is already associated with a different {@link JsonValueSpec}.
	 */
	public TypeScanner specify(DataType type, JsonValueSpec spec) {
		var old = inProgress.put(type, spec);
		if (old == null || old == spec) {
			return this;
		} else {
			throw new IllegalArgumentException("Multiple specifications for the same type " + type + ": " + old + " and " + spec);
		}
	}

	public TypeScanner specifyRecordFields(Class<? extends Record> type, Map<String, FixedMapMember> componentsByName) {
		var old = recordComponentOverrides.put(type, Map.copyOf(componentsByName));
		if (old != null) {
			throw new IllegalStateException("Already specified record fields for " + type);
		}
		return this;
	}

	public TypeMap build() {
		scanRefs();
		inProgress.freeze();
		TypeMap optimized = new Optimizer().optimize(inProgress);
		optimized.freeze();
		return optimized;
	}

	/**
	 * During the scans, we may have created some TypeRefNodes.
	 * Proactively scan those now, before optimization begins,
	 * to give the most possible information to the optimizer.
	 */
	private void scanRefs() {
		// The scan itself can generate more type references, thereby mutating refs,
		// so we can't simply iterate over refs.
		while (!refs.isEmpty()) {
			var iter = refs.entrySet().iterator();
			var entry = iter.next();
			iter.remove();
			scan(entry.getKey());
		}
	}

	private JsonValueSpec computeSpecNode(DataType type) {
		return switch (type) {
			case ArrayType t -> scanArray(t);
			case InstanceType t -> scanClass(t);
			case PrimitiveType t -> scanPrimitive(t);
			default -> throw new IllegalStateException("Unsupported type: " + type);
		};
	}

	private JsonValueSpec refNode(DataType type) {
		if (type instanceof KnownType k) {
			return refNode(k);
		} else {
			throw new IllegalStateException("Can't make a TypeRefNode for unknown type " + type);
		}
	}

	private JsonValueSpec refNode(KnownType type) {
		return refs.computeIfAbsent(type, TypeRefNode::new);
	}

	private JsonValueSpec scanArray(ArrayType array) {
		throw new IllegalStateException("Not implemented");
	}

	private JsonValueSpec scanPrimitive(PrimitiveType primitive) {
		var clazz = primitive.rawClass();
		if (PRIMITIVE_NUMBER_CLASSES.containsKey(clazz)) {
			return new PrimitiveNumberNode(clazz);
		}
		if (clazz == boolean.class) {
			return new BooleanNode();
		}
		throw new IllegalStateException("Unsupported primitive type: " + primitive);
	}

	private JsonValueSpec scanClass(InstanceType type) {
		var clazz = type.rawClass();
		if (clazz.isRecord()) {
			return scanRecord(clazz);
		}
		if (Number.class.isAssignableFrom(clazz)) {
			if (clazz == BigDecimal.class) {
				return new BigNumberNode(BigDecimal.class);
			}
			Class<?> primitiveClass = PRIMITIVE_NUMBER_CLASSES.entrySet().stream()
				.filter(e -> e.getValue() == clazz)
				.map(Map.Entry::getKey)
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("Unsupported number class: " + clazz));
			return new BoxedPrimitiveSpec(new PrimitiveNumberNode(primitiveClass));
		}
		if (Collection.class.isAssignableFrom(clazz) && clazz.isAssignableFrom(List.class)) {
			return new ArrayNode(
				refNode(type.parameterType(Collection.class, 0)),
				listAccumulator(type),
				listEmitter(type)
			);
		}
		if (Map.class.isAssignableFrom(clazz) && clazz.isAssignableFrom(Map.class)) {
			StringSpec keySpec = scanStringParsingClass((InstanceType) type.parameterType(Map.class, 0));
			JsonValueSpec valueSpec = refNode(type.parameterType(Map.class, 1));
			return new UniformMapNode(
				keySpec,
				valueSpec,
				mapAccumulator(type),
				mapEmitter(type)
			);
		}
		if (isStringParsingClass(type)) {
			return scanStringParsingClass(type);
		}
		throw new IllegalStateException("Not yet implemented");
	}

	private ArrayAccumulator listAccumulator(InstanceType type) {
		assert type.rawClass().isAssignableFrom(ArrayList.class);
		MethodHandle creator, listAdd, finisher;
		try {
			creator = MethodHandles.lookup().unreflectConstructor(ArrayList.class.getConstructor());
			listAdd = MethodHandles.lookup().unreflect(List.class.getMethod("add", Object.class));
			finisher = MethodHandles.lookup().unreflect(Collections.class.getMethod("unmodifiableList", List.class));
		} catch (IllegalAccessException | NoSuchMethodException e) {
			throw new IllegalStateException("Unexpected error doing reflection on List", e);
		}
		var upcastCreator = creator.asType(creator.type().changeReturnType(List.class));
		var integrator = listAdd.asType(listAdd.type().changeReturnType(void.class));
		var upcastFinisher = finisher.asType(finisher.type().changeReturnType(type.rawClass()));
		return new ArrayAccumulator(
			new TypedHandle(upcastCreator, DataType.of(List.class), List.of()),
			new TypedHandle(integrator, DataType.VOID, List.of(DataType.of(List.class), DataType.OBJECT)),
			new TypedHandle(upcastFinisher, type, List.of(DataType.of(List.class)))
		);
	}

	private ArrayEmitter listEmitter(InstanceType type) {
		assert List.class.isAssignableFrom(type.rawClass());
		if (!(type.parameterType(List.class, 0) instanceof KnownType elementType)) {
			throw new IllegalStateException("Can't emit from a list of unknown element type: " + type);
		}
		MethodHandle iterator, hasNext, next;
		try {
			iterator = MethodHandles.lookup().unreflect(List.class.getMethod("iterator"));
			hasNext = MethodHandles.lookup().unreflect(Iterator.class.getMethod("hasNext"));
			next = MethodHandles.lookup().unreflect(Iterator.class.getMethod("next"));
		} catch (IllegalAccessException | NoSuchMethodException e) {
			throw new IllegalStateException("Unexpected error doing reflection on List", e);
		}
		var downcastNext = next.asType(next.type().changeReturnType(elementType.rawClass()));
		return new ArrayEmitter(
			new TypedHandle(iterator, DataType.of(Iterator.class), List.of(type)),
			new TypedHandle(hasNext, DataType.BOOLEAN, List.of(DataType.of(Iterator.class))),
			new TypedHandle(downcastNext, elementType, List.of(DataType.of(Iterator.class)))
		);
	}

	private ObjectAccumulator mapAccumulator(InstanceType type) {
		assert type.rawClass().isAssignableFrom(LinkedHashMap.class);
		MethodHandle creator, mapPut, finisher;
		try {
			creator = MethodHandles.lookup().unreflectConstructor(LinkedHashMap.class.getConstructor());
			mapPut = MethodHandles.lookup().unreflect(Map.class.getDeclaredMethod("put", Object.class, Object.class));
			finisher = MethodHandles.lookup().unreflect(Collections.class.getMethod("unmodifiableMap", Map.class));
		} catch (IllegalAccessException | NoSuchMethodException e) {
			throw new IllegalStateException("Unexpected error doing reflection on List", e);
		}
		var upcastCreator = creator.asType(creator.type().changeReturnType(Map.class));
		var integrator = mapPut.asType(mapPut.type().changeReturnType(void.class));
		var upcastFinisher = finisher.asType(finisher.type().changeReturnType(type.rawClass()));
		return new ObjectAccumulator(
			new TypedHandle(upcastCreator, DataType.of(Map.class), List.of()),
			new TypedHandle(integrator, DataType.VOID, List.of(DataType.of(Map.class), DataType.OBJECT, DataType.OBJECT)),
			new TypedHandle(upcastFinisher, type, List.of(DataType.of(Map.class)))
		);
	}

	private ObjectEmitter mapEmitter(InstanceType type) {
		assert Map.class.isAssignableFrom(type.rawClass());
		if (!(type.parameterType(Map.class, 0) instanceof KnownType keyType) ||
			!(type.parameterType(Map.class, 1) instanceof KnownType valueType)) {
			throw new IllegalStateException("Can't emit from a map of unknown key or value type: " + type);
		}
		MethodHandle start, hasNext, next, getKey, getValue;
		try {
			start = MethodHandles.lookup().unreflect(TypeScanner.class.getDeclaredMethod("getIterator", Map.class));
			hasNext = MethodHandles.lookup().unreflect(Iterator.class.getMethod("hasNext"));
			next = MethodHandles.lookup().unreflect(Iterator.class.getMethod("next"));
			getKey = MethodHandles.lookup().unreflect(Map.Entry.class.getMethod("getKey"));
			getValue = MethodHandles.lookup().unreflect(Map.Entry.class.getMethod("getValue"));
		} catch (IllegalAccessException | NoSuchMethodException e) {
			throw new IllegalStateException("Unexpected error doing reflection on List", e);
		}
		var downcastGetKey = getKey.asType(getKey.type().changeReturnType(keyType.rawClass()));
		var downcastGetValue = getValue.asType(getValue.type().changeReturnType(valueType.rawClass()));
		var downcastNext = next.asType(next.type().changeReturnType(Map.Entry.class));
		return new ObjectEmitter(
			new TypedHandle(start, DataType.of(Iterator.class), List.of(DataType.of(Map.class))),
			new TypedHandle(hasNext, DataType.BOOLEAN, List.of(DataType.of(Iterator.class))),
			new TypedHandle(downcastNext, DataType.of(Map.Entry.class), List.of(DataType.of(Iterator.class))),
			new TypedHandle(downcastGetKey, keyType, List.of(DataType.of(Map.Entry.class))),
			new TypedHandle(downcastGetValue, valueType, List.of(DataType.of(Map.Entry.class)))
		);
	}

	private static Iterator<?> getIterator(Map<?,?> map) {
		return map.entrySet().iterator();
	}

	private static boolean isStringParsingClass(InstanceType type) {
		var clazz = type.rawClass();
		return clazz.isEnum() || clazz == String.class;
	}

	private static StringSpec scanStringParsingClass(InstanceType type) {
		assert isStringParsingClass(type);
		var clazz = type.rawClass();
		if (clazz.isEnum()) {
			return EnumByNameNode.of(clazz);
		}
		if (clazz == String.class) {
			return new StringNode();
		}
		throw new IllegalStateException("Unsupported type: " + clazz);
	}

	private JsonValueSpec scanRecord(Class<?> recordClass) {
		var componentOverrides = recordComponentOverrides.getOrDefault(recordClass, Map.of());
		SequencedMap<String, FixedMapMember> collect = Stream.of(recordClass.getRecordComponents())
			.collect(
				Collectors.toMap(
					RecordComponent::getName,
					c -> scanRecordComponent(c, componentOverrides),
					(_,_) -> { throw new IllegalStateException("Duplicate record component name"); },
					LinkedHashMap::new
				)
			);
		return new FixedMapNode(
			collect,
			recordFinisher(recordClass, collect)
		);
	}

	private TypedHandle recordFinisher(Class<?> recordClass, Map<String, FixedMapMember> componentsByName) {
		assert Record.class.isAssignableFrom(recordClass);
		assert componentsByName.keySet().equals(
			Stream.of(recordClass.getRecordComponents())
				.map(RecordComponent::getName)
				.collect(toSet())
		);
		Class<?>[] ctorParameterTypes = Stream.of(recordClass.getRecordComponents())
			.map(RecordComponent::getType)
			.toArray(Class<?>[]::new);
		MethodHandle constructor;
		try {
			constructor = MethodHandles.lookup().unreflectConstructor(recordClass.getConstructor(ctorParameterTypes));
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new IllegalStateException("Unexpected error accessing record constructor for " + recordClass, e);
		}
		return new TypedHandle(constructor, DataType.of(recordClass), componentsByName.values().stream().map(FixedMapMember::dataType).toList());
	}

	private FixedMapMember scanRecordComponent(RecordComponent c, Map<String, FixedMapMember> overrides) {
		if (overrides.get(c.getName()) instanceof FixedMapMember n) {
			return n;
		}
		MethodHandle mh;
		try {
			mh = MethodHandles.lookup().unreflect(c.getAccessor());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected error accessing record component accessor for " + c, e);
		}
		var accessor = new TypedHandle(mh, DataType.of(c.getType()), List.of(DataType.of(c.getDeclaringRecord())));

		DataType type = DataType.of(c.getGenericType());
		JsonValueSpec componentSpec = refNode(type);
		if (c.isAnnotationPresent(Nullable.class)) {
			return new FixedMapMember(new MaybeNullSpec(componentSpec), accessor);
		} else {
			return new FixedMapMember(componentSpec, accessor);
		}
	}

	public TypeMap.Settings settings() {
		return inProgress.settings();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(TypeScanner.class);
}
