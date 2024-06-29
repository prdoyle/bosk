package works.bosk.drivers.operations;

import works.bosk.BoskDriver;
import works.bosk.Identifier;
import works.bosk.MapValue;
import works.bosk.Reference;
import java.util.Collection;

public record SubmitConditionalDeletion<T>(
	Reference<T> target,
	Reference<Identifier> precondition,
	Identifier requiredValue,
	MapValue<String> diagnosticAttributes
) implements DeletionOperation<T>, ConditionalOperation {

	@Override
	public SubmitDeletion<T> unconditional() {
		return new SubmitDeletion<>(target, diagnosticAttributes);
	}

	@Override
	public SubmitConditionalDeletion<T> withFilteredAttributes(Collection<String> allowedNames) {
		return new SubmitConditionalDeletion<>(target, precondition, requiredValue, MapValue.fromFunction(allowedNames, diagnosticAttributes::get));
	}

	@Override
	public void submitTo(BoskDriver<?> driver) {
		driver.submitConditionalDeletion(target, precondition, requiredValue);
	}

}