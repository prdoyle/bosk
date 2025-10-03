package works.bosk.json.types;

import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public record TypeVariable(String name, List<? extends ParameterOrBound> upperBounds) implements UnknownType {

	public TypeVariable(String name, Type... upperBounds) {
		this(name, Stream.of(upperBounds).map(DeferredParameterOrBound::new).toList());
	}

	@Override
	public String toString() {
		if (upperBounds.isEmpty()) {
			return name;
		} else {
			return name + " extends "
				+ upperBounds.stream().map(ParameterOrBound::toString).collect(joining(" & "));
		}
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return other instanceof KnownType
			&& upperBounds.stream().allMatch(bound -> bound.dataType().isAssignableFrom(other));
	}
}
