package works.bosk;

import org.jspecify.annotations.Nullable;

/**
 * One update's hook work, captured when the update is applied.
 *
 * <p>
 * It uses the roots, update target, and diagnostic attributes captured at that
 * moment, not the values current at delivery time, because a standard-style
 * hook must see the state and context of the update that triggered it. The
 * range of hook registrations is frozen at that point too, so a hook registered
 * later does not receive an earlier update.
 *
 * <p>
 * Expanding the trigger runs each hook in that range as it finds it, so it can
 * wait for a hook, and hence can be interrupted. Expanding it into
 * {@link PendingHookSet} merely records the notifications.
 */
record HookTrigger<RR>(
	@Nullable RR priorRoot,
	RR rootForHook,
	Reference<?> target,
	MapValue<String> diagnosticAttributes,
	int hookStart,
	int hookEnd
) {}
