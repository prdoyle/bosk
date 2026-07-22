package works.bosk.drivers.mongo.internal;

import java.util.Optional;
import org.jspecify.annotations.Nullable;
import works.bosk.StateTreeNode;
import works.bosk.drivers.mongo.MongoDriverSettings.DatabaseFormat;
import works.bosk.drivers.mongo.MongoDriverSettings.SequoiaFormat;
import works.bosk.drivers.mongo.PandoFormat;

/**
 * Defines the format of the manifest document, which is stored in the database
 * to describe the database contents.
 *
 * @param version the version of the format of the manifest itself; currently must be 1
 * @param epoch a generation identifier for the database contents; changes when the collection is re-created
 * @param sequoia if present, indicates that the database is in the Sequoia format
 * @param pando if present, indicates that the database is in the Pando format and describes how it's configured
 */
public record Manifest(
	Integer version,
	@Nullable String epoch,
	Optional<EmptyNode> sequoia,
	Optional<PandoFormat> pando
) implements StateTreeNode {
	public Manifest {
		if (sequoia.isPresent() == pando.isPresent()) {
			throw new IllegalArgumentException("Exactly one format (sequoia or pando) must be specified in manifest");
		}
	}

	public record EmptyNode() implements StateTreeNode { }

	public static Manifest forSequoia(@Nullable String epoch) {
		return new Manifest(1, epoch, Optional.of(new EmptyNode()), Optional.empty());
	}

	public static Manifest forPando(PandoFormat settings, @Nullable String epoch) {
		return new Manifest(1, epoch, Optional.empty(), Optional.of(settings));
	}

	public static Manifest forFormat(DatabaseFormat format, @Nullable String epoch) {
		return switch (format) {
			case PandoFormat p -> forPando(p, epoch);
			case SequoiaFormat _ -> forSequoia(epoch);
		};
	}
}
