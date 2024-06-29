package works.bosk.drivers.operations;

import works.bosk.BoskDriver;
import works.bosk.MapValue;
import works.bosk.Reference;
import java.util.Collection;
import works.bosk.BoskDiagnosticContext;

public sealed interface UpdateOperation permits
	ConditionalOperation,
	DeletionOperation,
	ReplacementOperation
{
	Reference<?> target();
	MapValue<String> diagnosticAttributes();

	/**
	 * @return true if this operation matches <code>other</code> ignoring any preconditions.
	 */
	boolean matchesIfApplied(UpdateOperation other);

	UpdateOperation withFilteredAttributes(Collection<String> allowedNames);
	/**
	 * Calls the appropriate <code>submit</code> method on the given driver.
	 * Any {@link BoskDiagnosticContext diagnostic context} is <em>not</em> propagated;
	 * if that behaviour is desired, the caller must do it.
	 */
	void submitTo(BoskDriver<?> driver);
}
