package works.bosk.drivers.mongo.internal;

import java.util.Optional;
import works.bosk.Identifier;
import works.bosk.StateTreeNode;
import works.bosk.drivers.mongo.MongoDriverSettings.DatabaseFormat;
import works.bosk.drivers.mongo.MongoDriverSettings.SequoiaFormat;
import works.bosk.drivers.mongo.PandoFormat;

/**
 * Defines the format of the manifest document, which is stored in the database
 * to describe the database contents.
 *
 * @param version the version of the format of the manifest itself; currently must be 1
 * @param generation a UUID generated when the collection is initialized from scratch, and preserved thereafter; provides a "scope" for revision numbers. Nullable for backward compatibility
 * @param sequoia if present, indicates that the database is in the Sequoia format
 * @param pando if present, indicates that the database is in the Pando format and describes how it's configured
 */
public record Manifest(
	Integer version,
	Optional<Identifier> generation,
	Optional<EmptyNode> sequoia,
	Optional<PandoFormat> pando
) implements StateTreeNode {
	public Manifest {
		if (sequoia.isPresent() == pando.isPresent()) {
			throw new IllegalArgumentException("Exactly one format (sequoia or pando) must be specified in manifest");
		}
	}

	public record EmptyNode() implements StateTreeNode { }

	public static Manifest forSequoia(Optional<Identifier> generation) {
		return new Manifest(1, generation, Optional.of(new EmptyNode()), Optional.empty());
	}

	public static Manifest forPando(Optional<Identifier> generation, PandoFormat settings) {
		return new Manifest(1, generation, Optional.empty(), Optional.of(settings));
	}

	public static Manifest forFormat(Optional<Identifier> generation, DatabaseFormat format) {
		return switch (format) {
			case PandoFormat p -> forPando(generation, p);
			case SequoiaFormat _ -> forSequoia(generation);
		};
	}
}
