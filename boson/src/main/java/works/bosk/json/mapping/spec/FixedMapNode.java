package works.bosk.json.mapping.spec;

import java.util.Iterator;
import java.util.SequencedMap;
import works.bosk.json.mapping.spec.handles.TypedHandle;
import works.bosk.json.types.KnownType;

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

	@Override
	public KnownType dataType() {
		return finisher.returnType();
	}

}
