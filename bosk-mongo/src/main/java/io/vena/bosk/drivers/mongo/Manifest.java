package io.vena.bosk.drivers.mongo;

import io.vena.bosk.StateTreeNode;
import java.util.Optional;
import lombok.Value;

@Value
class Manifest implements StateTreeNode {
	Integer version;
	Optional<EmptyNode> sequoia;
	Optional<EmptyNode> pando;

	@Value
	public static class EmptyNode implements StateTreeNode {}

	public static Manifest forSequoia() {
		return new Manifest(1, Optional.of(new EmptyNode()), Optional.empty());
	}

	public static Manifest forPando() {
		return new Manifest(1, Optional.empty(), Optional.of(new EmptyNode()));
	}
}
