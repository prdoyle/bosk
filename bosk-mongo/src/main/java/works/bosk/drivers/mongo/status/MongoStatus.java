package works.bosk.drivers.mongo.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import works.bosk.StateTreeNode;
import works.bosk.drivers.mongo.MongoDriverSettings.DatabaseFormat;
import works.bosk.drivers.mongo.internal.Manifest;
import works.bosk.util.PerTenantValue;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Info about the state of the database, highlighting how it differs from the desired state.
 * TODO: Unit tests
 */
public record MongoStatus(
	@JsonInclude(NON_NULL) String error,
	ManifestStatus manifest,
	PerTenantValue<StateStatus> state
) {
	public MongoStatus with(DatabaseFormat preferredFormat, StateTreeNode actualManifest) {
		// The expected manifest should match the actual manifest's epoch so that
		// isAllClear() only reports format differences, not epoch differences.
		String epoch = actualManifest instanceof Manifest m ? m.epoch() : null;
		return new MongoStatus(
			this.error,
			new ManifestStatus(
				Manifest.forFormat(preferredFormat, epoch),
				actualManifest
			),
			this.state
		);
	}

	public boolean isAllClear() {
		return manifest.isIdentical()
			&& state.allMatch(s -> s.difference() instanceof NoDifference);
	}
}
