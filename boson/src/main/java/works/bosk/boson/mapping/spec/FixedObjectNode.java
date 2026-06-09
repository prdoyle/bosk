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
public record FixedObjectNode(
	SequencedMap<String, RecognizedMember> memberSpecs,
	TypedHandle finisher
) implements ObjectSpec {
	public FixedObjectNode {
		assert finisher.parameterTypes().size() == memberSpecs.size();
		Iterator<? extends DataType> iter = finisher.parameterTypes().iterator();
		memberSpecs.forEach((name, member) -> {
			DataType finisherParameter = iter.next();
			assert finisherParameter.isAssignableFrom(member.valueSpec().dataType()):
				"Finisher parameter type " + finisherParameter +
				" must be assignable from member '" + name +
				"' value type " + member.valueSpec().dataType();
		});
	}

	/**
	 * Less stringent way to make a {@link FixedObjectNode}, where the finisher
	 * accepts a single array of {@link Object} instead of the individual parameters.
	 * This will naturally be less efficient, as it requires an array allocation.
	 */
	public static FixedObjectNode withArrayFinisher(
		DataType resultType,
		SequencedMap<String, RecognizedMember> memberSpecs,
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
					.map(RecognizedMember::valueSpec)
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
		return new FixedObjectNode(memberSpecs, collectorHandle);
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
	public FixedObjectNode specialize(Map<String, DataType> actualArguments) {
		var memberSpecs = new LinkedHashMap<String, RecognizedMember>();
		this.memberSpecs.forEach((name, member) ->
			memberSpecs.put(name, member.substitute(actualArguments)));
		return new FixedObjectNode(
			memberSpecs,
			this.finisher.substitute(actualArguments)
		);
	}

	public abstract static class OneMemberWrangler<V, M1> {
		final String member1Name;

		protected OneMemberWrangler(String member1Name) {
			this.member1Name = member1Name;
		}

		public abstract M1 accessor(V value);
		public abstract V finish(M1 member1);
	}

	public abstract static class TwoMemberWrangler<V, M1, M2> {
		final String member1Name;
		final String member2Name;

		protected TwoMemberWrangler(String member1Name, String member2Name) {
			this.member1Name = member1Name;
			this.member2Name = member2Name;
		}

		public abstract M1 accessor1(V value);
		public abstract M2 accessor2(V value);
		public abstract V finish(M1 member1, M2 member2);
	}

	public static FixedObjectNode of(OneMemberWrangler<?, ?> wrangler) {
		BoundType wranglerType = (BoundType) DataType.known(wrangler.getClass());
		DataType valueType = wranglerType.parameterType(OneMemberWrangler.class, 0);

		var memberSpecs = new LinkedHashMap<String, RecognizedMember>();
		DataType arg1Type = wranglerType.parameterType(OneMemberWrangler.class, 1);
		memberSpecs.put(wrangler.member1Name, new RecognizedMember(
			new TypeRefNode(arg1Type),
			new TypedHandle(
				WRANGLER1_ACCESSOR1.bindTo(wrangler).asType(
					methodType(arg1Type.leastUpperBoundClass(), valueType.leastUpperBoundClass())
				),
				arg1Type, List.of(valueType)
			)
		));
		var finisher = new TypedHandle(
			WRANGLER1_FINISH.bindTo(wrangler).asType(
				methodType(
					valueType.leastUpperBoundClass(),
					arg1Type.leastUpperBoundClass()
				)
			),
			valueType, List.of(arg1Type)
		);
		return new FixedObjectNode(memberSpecs, finisher);
	}

	public static FixedObjectNode of(TwoMemberWrangler<?, ?,?> wrangler) {
		BoundType wranglerType = (BoundType) DataType.known(wrangler.getClass());
		DataType valueType = wranglerType.parameterType(TwoMemberWrangler.class, 0);

		var memberSpecs = new LinkedHashMap<String, RecognizedMember>();
		DataType arg1Type = wranglerType.parameterType(TwoMemberWrangler.class, 1);
		DataType arg2Type = wranglerType.parameterType(TwoMemberWrangler.class, 2);
		memberSpecs.put(wrangler.member1Name, new RecognizedMember(
			new TypeRefNode(arg1Type),
			new TypedHandle(
				WRANGLER2_ACCESSOR1.bindTo(wrangler).asType(
					methodType(arg1Type.leastUpperBoundClass(), valueType.leastUpperBoundClass())
				),
				arg1Type, List.of(valueType)
			)
		));
		memberSpecs.put(wrangler.member2Name, new RecognizedMember(
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
		return new FixedObjectNode(memberSpecs, finisher);
	}

	private static final MethodHandle WRANGLER1_ACCESSOR1;
	private static final MethodHandle WRANGLER1_FINISH;
	private static final MethodHandle WRANGLER2_ACCESSOR1;
	private static final MethodHandle WRANGLER2_ACCESSOR2;
	private static final MethodHandle WRANGLER2_FINISH;

	static {
		var lookup = MethodHandles.lookup();
		try {
			WRANGLER1_ACCESSOR1 = lookup.findVirtual(OneMemberWrangler.class, "accessor",
				methodType(Object.class, Object.class));
			WRANGLER1_FINISH = lookup.findVirtual(OneMemberWrangler.class, "finish",
				methodType(Object.class, Object.class));
			WRANGLER2_ACCESSOR1 = lookup.findVirtual(TwoMemberWrangler.class, "accessor1",
				methodType(Object.class, Object.class));
			WRANGLER2_ACCESSOR2 = lookup.findVirtual(TwoMemberWrangler.class, "accessor2",
				methodType(Object.class, Object.class));
			WRANGLER2_FINISH = lookup.findVirtual(TwoMemberWrangler.class, "finish",
				methodType(Object.class, Object.class, Object.class));
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
