package works.bosk.boson.mapping.spec;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.function.Function;
import works.bosk.boson.mapping.spec.handles.TypedHandle;
import works.bosk.boson.mapping.spec.handles.TypedHandles;
import works.bosk.boson.types.BoundType;
import works.bosk.boson.types.DataType;
import works.bosk.boson.types.KnownType;
import works.bosk.boson.types.TypeReference;

import static java.lang.invoke.MethodType.methodType;

// TODO: Find a way to deal with unrecognized members
public record FixedMapNode(
	SequencedMap<String, FixedMapMember> memberSpecs,
	TypedHandle finisher
) implements ObjectSpec {
	public FixedMapNode {
		assert finisher.parameterTypes().size() == memberSpecs.size();
		Iterator<? extends DataType> iter = finisher.parameterTypes().iterator();
		memberSpecs.forEach((name, member) -> {
			DataType finisherParameter = iter.next();
			assert finisherParameter.isAssignableFrom(member.valueSpec().dataType()):
				"Finisher parameter type " + finisherParameter +
				" is not assignable from member '" + name +
				"' value type " + member.valueSpec().dataType();
		});
	}

	/**
	 * Less stringent way to make a {@link FixedMapNode}, where the finisher
	 * accepts a single array of {@link Object} instead of the individual parameters.
	 * This will naturally be less efficient, as it requires an array allocation.
	 */
	public static FixedMapNode withArrayFinisher(
		DataType resultType,
		SequencedMap<String, FixedMapMember> memberSpecs,
		Function<Object[], ?> arrayFinisher
	) {
		KnownType objectArray = DataType.known(new TypeReference<Object[]>() {});
		var finisherHandle = TypedHandles.function(
			objectArray,
			resultType,
			arrayFinisher);
		var collectorMH = finisherHandle.handle()
			.asCollector(Object[].class, memberSpecs.size());
		var castMH = collectorMH.asType(
			methodType(resultType.leastUpperBoundClass(),
				memberSpecs.values().stream()
					.map(FixedMapMember::valueSpec)
					.map(SpecNode::dataType)
					.map(DataType::leastUpperBoundClass)
					.toArray(Class<?>[]::new)
			)
		);
		var collectorHandle = new TypedHandle(
			castMH,
			finisherHandle.returnType(),
			memberSpecs.values().stream()
				.map(m -> m.valueSpec().dataType())
				.toList()
		);
		return new FixedMapNode(memberSpecs, collectorHandle);
	}

	@Override
	public DataType dataType() {
		return finisher.returnType();
	}

	@Override
	public String briefIdentifier() {
		return "Fixed_" + dataType().leastUpperBoundClass().getSimpleName();
	}

	@Override
	public FixedMapNode substitute(Map<String, DataType> actualArguments) {
		var memberSpecs = new LinkedHashMap<String, FixedMapMember>();
		this.memberSpecs.forEach((name, member) ->
			memberSpecs.put(name, member.substitute(actualArguments)));
		return new FixedMapNode(
			memberSpecs,
			this.finisher.substitute(actualArguments)
		);
	}

	public interface Wrangler2<T1, T2, V> {
		T1 accessor1(V value);
		T2 accessor2(V value);
		V finish(T1 arg1, T2 arg2);
	}

	public static FixedMapNode of(Wrangler2<?,?,?> wrangler) {
		BoundType wranglerType = (BoundType) DataType.known(wrangler.getClass());
		DataType arg1Type = wranglerType.parameterType(Wrangler2.class, 0);
		DataType arg2Type = wranglerType.parameterType(Wrangler2.class, 1);
		DataType valueType = wranglerType.parameterType(Wrangler2.class, 2);
		var memberSpecs = new LinkedHashMap<String, FixedMapMember>();
		memberSpecs.put("arg1", new FixedMapMember(
			new TypeRefNode(arg1Type),
			new TypedHandle(
				WRANGLER2_ACCESSOR1.bindTo(wrangler).asType(
					methodType(arg1Type.leastUpperBoundClass(), valueType.leastUpperBoundClass())
				),
				arg1Type, List.of(valueType)
			)
		));
		memberSpecs.put("arg2", new FixedMapMember(
			new TypeRefNode(arg2Type),
			new TypedHandle(
				WRANGLER2_ACCESSOR2.bindTo(wrangler).asType(
					methodType(arg2Type.leastUpperBoundClass(), valueType.leastUpperBoundClass())
				),
				arg2Type, List.of(valueType)
			)
		));
		var finisher = new TypedHandle(
			WRANGLER2_FINISH.bindTo(wrangler).asType(
				methodType(
					valueType.leastUpperBoundClass(),
					arg1Type.leastUpperBoundClass(),
					arg2Type.leastUpperBoundClass()
				)
			),
			valueType, List.of(arg1Type, arg2Type)
		);
		return new FixedMapNode(memberSpecs, finisher);
	}

	private static final MethodHandle WRANGLER2_ACCESSOR1;
	private static final MethodHandle WRANGLER2_ACCESSOR2;
	private static final MethodHandle WRANGLER2_FINISH;

	static {
		var lookup = MethodHandles.lookup();
		try {
			WRANGLER2_ACCESSOR1 = lookup.findVirtual(Wrangler2.class, "accessor1",
				methodType(Object.class, Object.class));
			WRANGLER2_ACCESSOR2 = lookup.findVirtual(Wrangler2.class, "accessor2",
				methodType(Object.class, Object.class));
			WRANGLER2_FINISH = lookup.findVirtual(Wrangler2.class, "finish",
				methodType(Object.class, Object.class, Object.class));
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
