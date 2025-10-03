package works.bosk.json.mapping.spec;

import java.util.Iterator;
import java.util.SequencedMap;
import java.util.function.Function;
import works.bosk.json.mapping.spec.handles.TypedHandle;
import works.bosk.json.types.DataType;
import works.bosk.json.types.InstanceType;
import works.bosk.json.types.KnownType;

import static java.lang.invoke.MethodType.methodType;

// TODO: Find a way to deal with unrecognized members
public record FixedMapNode(
	SequencedMap<String, FixedMapMember> memberSpecs,
	TypedHandle finisher
) implements ObjectSpec {
	public FixedMapNode {
		assert finisher.parameterTypes().size() == memberSpecs.size();
		Iterator<KnownType> iter = finisher.parameterTypes().iterator();
		memberSpecs.values().forEach(member -> {
			assert iter.next().isAssignableFrom(member.valueSpec().dataType());
		});
	}

	/**
	 * Less stringent way to make a {@link FixedMapNode}, where the finisher
	 * accepts a single array of {@link Object} instead of the individual parameters.
	 * This will naturally be less efficient, as it requires an array allocation.
	 */
	public static <T> FixedMapNode withArrayFinisher(
		SequencedMap<String, FixedMapMember> memberSpecs,
		Function<T[], ?> arrayFinisher
	) {
		InstanceType finisherType = (InstanceType) DataType.of(arrayFinisher.getClass());
		Class<?> finisherArgumentArrayClass = finisherType.parameterBinding(Function.class, 0).knownType().rawClass();
		var finisherHandle = TypedHandle.of(arrayFinisher);
		var collectorMH = finisherHandle.handle()
			.asCollector(finisherArgumentArrayClass, memberSpecs.size());
		var castMH = collectorMH.asType(
			methodType(finisherHandle.returnType().rawClass(),
				memberSpecs.values().stream()
					.map(FixedMapMember::valueSpec)
					.map(SpecNode::dataType)
					.map(KnownType::rawClass)
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
	public KnownType dataType() {
		return finisher.returnType();
	}

}
