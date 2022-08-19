package org.vena.bosk;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Called to indicate the hook's "scope object" may have been modified.
 */
public interface BoskHook<T> {
	/**
	 * @param reference points to an object that may have been modified, corresponding
	 * to the scope on which this hook was registered. The referenced object may or
	 * may not exist.
	 */
	void onChanged(Reference<T> reference);

	/**
	 * Creates a {@link BoskHook} implementing a simple closed-loop controller with the following logic:
	 *
	 * <pre>
	 * return reference -> {
	 * 	PV desiredSetpoint = setpoint.apply(reference.valueIfExists());
	 * 	PV existingSetpoint = sensor.get();
	 * 	if (!Objects.equals(existingSetpoint, desiredSetpoint)) {
	 * 		actuator.accept(desiredSetpoint);
	 * 	}
	 * };
	 * </pre>
	 *
	 * This pattern is a common one with a number of desirable properties,
	 * so we provide this utility function to encourage its use.
	 *
	 * @param setpoint {@link Function} taking the value of the hooked bosk node and returning the desired setpoint
	 * @param sensor {@link Supplier} returning the current effective setpoint value (the "process variable") based on an observation of the system being controlled
	 * @param actuator {@link Consumer} that accepts a new setpoint and takes the desired action on the system being controlled
	 * @param <N> type of the bosk state node being watched by the hook
	 * @param <PV> type of the "process variable" for the controller
	 */
	static <N, PV> BoskHook<N> controller(
		Function<N, PV> setpoint,
		Supplier<PV> sensor,
		Consumer<PV> actuator
	) {
		return reference -> {
			PV desiredSetpoint = setpoint.apply(reference.valueIfExists());
			PV existingSetpoint = sensor.get();
			if (!Objects.equals(existingSetpoint, desiredSetpoint)) {
				actuator.accept(desiredSetpoint);
			}
		};
	}
}
