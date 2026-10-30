package works.bosk;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pending hook calls, with one bit per hook registration and reference.
 *
 * <p>
 * Each path keeps one bit per hook registration. A hook's first pending
 * notification records whether the reference existed before that notification.
 * If a reference is created and then deleted before the hook runs, the pending
 * bit is removed: the hook never observed the reference and has nothing to do.
 *
 * <p>
 * Notifications accumulate in the <em>active</em> set until a batch detaches
 * them. The detached set is delivered outside this object's monitor, while the
 * next active set accepts notifications for changes that arrive during
 * delivery.
 *
 * <p>
 * Compacted notifications do not carry diagnostic attributes because there is
 * no single originating update for them.
 */
final class PendingHookSet {
	private Map<Path, PendingHooks> active = new LinkedHashMap<>();

	/**
	 * Adds a pending hook notification.
	 *
	 * @param hookIndex     the registration's stable index, as reported by
	 *                      {@link HookRunner.HookRegistration#index()}
	 * @param path          the path of the reference that changed
	 * @param existedBefore whether the reference existed before the change, as
	 *                      {@link Reference#exists()} would have reported
	 * @param existsAfter   whether the reference exists after the change
	 */
	synchronized void add(int hookIndex, Path path, boolean existedBefore, boolean existsAfter) {
		assert existedBefore || existsAfter : "A hook notification must describe an actual change";
		PendingHooks hooks = active.get(path);
		if (hooks == null) {
			hooks = new PendingHooks(new BitSet(), new BitSet());
			active.put(path, hooks);
		}
		if (hooks.toRun().get(hookIndex)) {
			if (hooks.existedAtBaseline().get(hookIndex) || existsAfter) {
				return;
			}
			// The hook never saw this reference: it was absent at baseline and is gone now.
			hooks.toRun().clear(hookIndex);
			hooks.existedAtBaseline().clear(hookIndex);
			if (hooks.toRun().isEmpty()) {
				active.remove(path);
			}
		} else {
			hooks.toRun().set(hookIndex);
			hooks.existedAtBaseline().set(hookIndex, existedBefore);
		}
	}

	synchronized boolean isEmpty() {
		return active.isEmpty();
	}

	/**
	 * Adds notifications that were observed earlier than the ones already in this
	 * set, as when a batch is put back after delivery failed.
	 *
	 * <p>
	 * If this set already has a notification for the same pair, the earlier batch's
	 * baseline wins, because its observation came first.
	 */
	synchronized void mergeEarlier(Map<Path, PendingHooks> earlierBatch) {
		for (var entry : earlierBatch.entrySet()) {
			PendingHooks earlier = entry.getValue();
			if (earlier.toRun().isEmpty()) {
				continue;
			}
			PendingHooks target = active.computeIfAbsent(entry.getKey(), _ -> new PendingHooks(new BitSet(), new BitSet()));
			for (int hookIndex = earlier.toRun().nextSetBit(0); hookIndex >= 0; hookIndex = earlier.toRun().nextSetBit(hookIndex + 1)) {
				target.toRun().set(hookIndex);
				target.existedAtBaseline().set(hookIndex, earlier.existedAtBaseline().get(hookIndex));
			}
		}
	}

	/**
	 * Detaches the active notifications for delivery and starts a fresh active
	 * set. The returned map is no longer modified by this object.
	 */
	synchronized Map<Path, PendingHooks> detach() {
		Map<Path, PendingHooks> result = active;
		active = new LinkedHashMap<>();
		return result;
	}

	synchronized void clear() {
		active = new LinkedHashMap<>();
	}

	record PendingHooks(BitSet toRun, BitSet existedAtBaseline) {}
}
