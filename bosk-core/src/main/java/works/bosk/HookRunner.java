package works.bosk;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.holdsLock;
import static java.util.Objects.requireNonNull;
import static works.bosk.logging.MappedDiagnosticContext.setupMDC;

/**
 * Hook delivery: which hooks an update triggers, and running them in order.
 *
 * <p>
 * The local driver calls {@link #update} as it applies each update, and
 * {@link #drainIfAllowed} afterwards to run the resulting calls.
 *
 * <p>
 * The driver's monitor guards this object, because accepting a trigger has to
 * happen atomically with the state change that produced it, so that hooks run in
 * the order their updates were applied. The driver holds its monitor when calling
 * {@link #update} and {@link #register}; everything else takes the monitor itself.
 */
final class HookRunner<R extends StateTreeNode> {
	private final Bosk<R> bosk;
	private final Object monitor;
	private final List<HookRegistration<?>> hooks = new ArrayList<>();
	private final Deque<HookTrigger<R>> hookQueue = new ConcurrentLinkedDeque<>();
	private final Semaphore drainPermit = new Semaphore(1);
	private final Thread.Builder hookThreadBuilder = Thread
		.ofVirtual()
		.name("bosk-hook-", 1);

	HookRunner(Bosk<R> bosk, Object monitor) {
		this.bosk = bosk;
		this.monitor = monitor;
	}

	/**
	 * Adds the given hook to the registry and runs it on every existing object
	 * that matches its scope.
	 *
	 * <p>
	 * The caller must hold the monitor, so that the registration is ordered with
	 * respect to updates.
	 */
	<T> void register(String name, Reference<T> scope, BoskHook<T> action) {
		assert holdsLock(monitor);
		HookRegistration<T> reg = new HookRegistration<>(name, scope, action, hooks.size());
		hooks.add(reg);
		accept(new HookTrigger<>(
			null,
			requireNonNull(bosk.currentRoot()),
			bosk.rootRef,
			bosk.context.getAttributes(),
			reg.index(),
			reg.index() + 1));
	}

	/**
	 * Records the hook work for an update that has just been applied, without
	 * running any hooks yet.
	 *
	 * <p>
	 * The caller must hold the monitor, so that hook work stays in update order.
	 *
	 * @param priorRoot the state before the update, for detecting which references
	 *                  changed
	 * @param target    the reference the update was applied to
	 */
	void update(@Nullable R priorRoot, Reference<?> target) {
		assert holdsLock(monitor);
		R rootForHook = bosk.currentRoot();
		int hookEnd = hooks.size();
		boolean anyMatch = hooks.subList(0, hookEnd).stream()
			.anyMatch(hook -> hook.matchesTarget(target));
		if (anyMatch) {
			accept(new HookTrigger<>(
				priorRoot,
				rootForHook,
				target,
				bosk.context.getAttributes(),
				0,
				hookEnd));
		}
	}

	/**
	 * Runs queued hooks in a "breadth-first" fashion: all hooks "H" triggered by
	 * any single hook "G" will run before any consequent hooks triggered by "H".
	 *
	 * <p>
	 * The <a href="https://en.wikipedia.org/w/index.php?title=Breadth-first_search&oldid=1059916234#Pseudocode">classic BFS algorithm</a>
	 * has an outer loop that dequeues nodes for processing; however, we have an
	 * "inversion of control" situation here, where the bosk is not in control of
	 * the outermost loop.
	 *
	 * <p>
	 * Instead, we maintain a semaphore to distinguish "outermost calls" from
	 * "recursive calls", and dequeue nodes only at the outermost level, thereby
	 * effectively implementing the classic BFS algorithm despite not having access
	 * to the outermost loop of the application. The dequeuing is "allowed" only
	 * at the outermost level.
	 *
	 * <p>
	 * As a side-benefit, this also provides thread safety, as well as intuitive behaviour
	 * in the presence of parallelism.
	 *
	 * <p>
	 * Note: don't call while holding the monitor. Running hooks means running arbitrary
	 * user code, which can take an arbitrary amount of time, and if the monitor is held,
	 * that blocks other threads from submitting updates.
	 */
	void drainIfAllowed() {
		do {
			if (drainPermit.tryAcquire()) {
				try {
					LOGGER.debug("Draining hook work");
					while (true) {
						// An interrupt means "stop", not "pause": we do not
						// resume the backlog on some later, unrelated update.
						if (Thread.currentThread().isInterrupted()) {
							LOGGER.debug("Interrupted; clearing the remaining queued hooks");
							hookQueue.clear();
							return;
						}
						HookTrigger<R> work = hookQueue.pollFirst();
						if (work == null) {
							LOGGER.debug("Hook queue is empty");
							break;
						}
						try {
							execute(work);
						} catch (InterruptedException e) {
							hookQueue.clear();
							Thread.currentThread().interrupt();
							LOGGER.warn("Interrupted while running hooks; the running hook was interrupted and terminated, and the remaining queued hooks were discarded", e);
							return;
						}
					}
				} finally {
					drainPermit.release();
				}
			} else {
				LOGGER.debug("Not draining the hook queue");
				return;
			}

			// The do-while loop here needs an explanation. At this location in the code,
			// we need to check again whether the queue is empty. Here's why.
			//
			// Events:
			//  - Q: Queue a hook
			//  - A: Acquire the permit
			//  - D: Drain the queue till it's empty
			//  - R: Release the permit
			//  - F: Try to acquire the permit and fail
			//
			// The two threads:
			//   This thread        Other thread
			//        Q
			//        A
			//        D
			//                         Q
			//                         F
			//        R
			//        * <-- (You are here)
			//
			// At this point, the queue may not be empty, yet this thread thinks it's drained,
			// and the other thread thinks we'll drain it.
			//
			// Fortunately, the solution is simple: just check again. If the queue is empty
			// at this point, we can safely stop running hooks, secure in the knowledge that
			// if another thread queues another hook after this point, that thread will also
			// succeed in acquiring the permit and will itself drain the queue.

		} while (!hookQueue.isEmpty());
	}

	/**
	 * Adds a trigger to the queue for standard-style delivery.
	 */
	private void accept(HookTrigger<R> trigger) {
		assert holdsLock(monitor);
		hookQueue.addLast(trigger);
	}

	private void execute(HookTrigger<R> trigger) throws InterruptedException {
		for (int i = trigger.hookStart(); i < trigger.hookEnd(); i++) {
			runHookFor(hookAt(i), trigger.priorRoot(), trigger.rootForHook(), trigger.target(), trigger.diagnosticAttributes());
		}
	}

	private <S> void runHookFor(HookRegistration<S> reg, @Nullable R priorRoot, R rootForHook, Reference<?> target, MapValue<String> diagnosticAttributes) throws InterruptedException {
		reg.triggerAction(priorRoot, rootForHook, target, (changedRef, existedBefore, existsAfter) -> {
			LOGGER.debug("Hook: trigger {}({}) due to {}", reg.name(), changedRef, target);
			run(reg.name(), reg.hook(), changedRef, rootForHook, diagnosticAttributes);
		});
	}

	private HookRegistration<?> hookAt(int index) {
		synchronized (monitor) {
			HookRegistration<?> registration = hooks.get(index);
			assert registration.index() == index;
			return registration;
		}
	}

	/**
	 * Runs the given hook, and does not return until it has finished.
	 *
	 * @param hookName             the registration's name, for diagnostics
	 * @param changedRef           the reference the hook is being told about
	 * @param rootForHook          the state the hook's read session will see
	 * @param diagnosticAttributes the context attributes of the triggering update
	 */
	private <S> void run(String hookName, BoskHook<S> hook, Reference<S> changedRef, R rootForHook, MapValue<String> diagnosticAttributes) throws InterruptedException {
		FutureTask<Void> task = new FutureTask<>(() -> {
			// We use two nested try statements here so that the "finally" clause runs within the diagnostic scope
			try (
				var _ = setupMDC(bosk.name, bosk.instanceID);
				var _ = bosk.context.withOnly(diagnosticAttributes)
			) {
				try (var _ = bosk.newReadSession(rootForHook)) {
					LOGGER.debug("Hook: RUN {}({})", hookName, changedRef);
					hook.onChanged(changedRef);
				} catch (InterruptedException e) {
					LOGGER.warn("Bosk hook \"{}\" was interrupted; proceeding", hookName, e);
				} catch (RuntimeException e) {
					LOGGER.error("Bosk hook \"{}\" terminated with an exception, which usually indicates a bug. State updates may have been lost", hookName, e);

					// Note that we don't catch Error. The practical reason is to allow users to write
					// unit tests that throw AssertionError from hooks, but the bigger reason is that
					// Errors indicate that something has gone dreadfully wrong, and we probably should
					// not attempt to continue.
				} finally {
					LOGGER.debug("Hook: end {}({})", hookName, changedRef);
				}
			}
		}, null);
		// A fresh virtual thread keeps thread locals from the triggering thread
		// out of user code. This is slightly tragic, because usually thread-local
		// propagation works just the way we'd want it, but not always. Given the
		// choices "always, sometimes, never", if we can't achieve "always", then
		// the bosk philosophy prefers "never" over "sometimes".
		Thread hookThread = hookThreadBuilder.start(task);
		try {
			task.get();
		} catch (ExecutionException e) {
			try {
				throw e.getCause();
			} catch (RuntimeException | Error cause) {
				throw cause;
			} catch (Throwable t) {
				throw new AssertionError("Hook runnable should catch and wrap checked exceptions", t);
			}
		} catch (InterruptedException e) {
			// The interrupt is intended for the hook work in flight here. Deliver it
			// to the running hook, per the BoskHook contract, and await its termination
			// before proceeding. This mimics structured concurrency: cancel the
			// in-flight subtask and wait for it to finish.
			hookThread.interrupt();
			awaitTermination(hookThread);
			throw e;
		}
	}

	/**
	 * Waits for the given hook thread to terminate. If this (the calling) thread is
	 * interrupted while waiting, keep waiting: the interrupt has already been
	 * delivered to the hook, and proceeding without it would leave the hook running
	 * orphaned.
	 */
	private void awaitTermination(Thread hookThread) {
		while (true) {
			try {
				hookThread.join();
				return;
			} catch (InterruptedException e) {
				// Keep waiting; the interrupt has been delivered to the hook already.
			}
		}
	}

	/**
	 * Recursive helper routine that calls the given action for all objects matching <code>effectiveScope</code> that
	 * are different between <code>priorRoot</code> and <code>newRoot</code>.
	 * Each level of recursion fills in one parameter in <code>effectiveScope</code>;
	 * for the base case, this calls <code>action</code> unless the prior and current values are the same object.
	 *
	 * @param effectiveScope The hook scope with zero or more of its parameters filled in
	 * @param priorRoot      The root before the change that triggered the hook; or null during initialization when running
	 *                       hooks on the {@link BoskDriver#initialState initial state}.
	 * @param newRoot        The root after the change that triggered the hook. This will be the root in the {@link Bosk.ReadSession}
	 *                       during hook execution.
	 * @param action         The operation to perform for each matching object that is different between the two roots
	 * @param <S>            The type of the hook scope object
	 */
	private <S> void triggerCascade(Reference<S> effectiveScope, @Nullable R priorRoot, R newRoot, HookAction<S> action) throws InterruptedException {
		if (effectiveScope.path().numParameters() == 0) {
			// effectiveScope points at a single node that may have changed
			//
			S priorValue = refValueIfExists(effectiveScope, priorRoot);
			S currentValue = refValueIfExists(effectiveScope, newRoot);
			if (priorValue == currentValue) { // Note object identity comparison
				LOGGER.debug("Hook: skip unchanged {}", effectiveScope);
			} else {
				// We've found something that changed
				action.execute(effectiveScope, priorValue != null, currentValue != null);
			}
		} else {
			// There's at least one parameter that hasn't been bound yet. This means
			// we need to locate all the matching objects that may have changed.
			// We do so by filling in the first parameter with all possible values that
			// could correspond to changed objects and then recursing.
			//
			Reference<EnumerableByIdentifier<?>> containerRef = effectiveScope.truncatedBeforeFirstParameter();
			EnumerableByIdentifier<?> priorContainer = refValueIfExists(containerRef, priorRoot);
			EnumerableByIdentifier<?> newContainer = refValueIfExists(containerRef, newRoot);

			// TODO: If priorContainer == newContainer, can we stop immediately?

			// Process any deleted items first. This can allow the hook to free some memory
			// that can be used by subsequent hooks.
			// We do them in reverse order just because that's likely to be the preferred
			// order for cleanup activities.
			//
			// TODO: Should we actually process the hooks themselves in reverse order for the same reason?
			//
			if (priorContainer != null) {
				List<Identifier> priorIDs = priorContainer.ids();
				for (Identifier id : priorIDs.reversed()) {
					if (newContainer == null || newContainer.get(id) == null) {
						triggerCascade(effectiveScope.boundTo(id), priorRoot, newRoot, action);
					}
				}
			}

			// Then process updated items
			//
			if (newContainer != null) {
				for (Identifier id : newContainer.ids()) {
					if (priorContainer == null || priorContainer.get(id) != newContainer.get(id)) {
						triggerCascade(effectiveScope.boundTo(id), priorRoot, newRoot, action);
					}
				}
			}
		}
	}

	@Nullable
	private <V> V refValueIfExists(Reference<V> ref, @Nullable R root) {
		if (root == null) {
			return null;
		} else {
			// TODO: This would be less cumbersome if we could apply a Reference to an arbitrary root object.
			// For now, References only apply to the current ReadSession, so we need a new ReadSession every time
			// we want to change roots.
			try (var _ = bosk.newReadSession(root)) {
				return ref.valueIfExists();
			}
		}
	}

	/**
	 * Receives each reference whose value changed, along with its existence
	 * before and after the change.
	 */
	@FunctionalInterface
	interface HookAction<S> {
		void execute(Reference<S> ref, boolean existedBefore, boolean existsAfter) throws InterruptedException;
	}

	// Inner class can't be a record
	final class HookRegistration<S> {
		private final String name;
		private final Reference<S> scope;
		private final BoskHook<S> hook;
		private final int index;

		HookRegistration(String name, Reference<S> scope, BoskHook<S> hook, int index) {
			this.name = name;
			this.scope = scope;
			this.hook = hook;
			this.index = index;
		}

		/**
		 * @return whether this hook's scope matches the path of {@code target}
		 */
		boolean matchesTarget(Reference<?> target) {
			int relativeDepth = target.path().length() - scope.path().length();
			if (relativeDepth >= 0) {
				// target may be the scope object or a descendant
				Path candidate = target.path().truncatedBy(relativeDepth);
				return scope.path().matches(candidate);
			} else {
				// target may be an ancestor of the scope object
				Path enclosingScope = scope.path().truncatedBy(-relativeDepth);
				return enclosingScope.matches(target.path());
			}
		}

		/**
		 * Calls <code>action</code> for every object whose path matches <code>scope</code> that
		 * was changed by a driver event targeting <code>target</code>.
		 *
		 * @param priorRoot The bosk root object before the driver event occurred
		 * @param newRoot   The bosk root object after the driver event occurred
		 * @param target    The object specified by the driver event
		 * @param action    The operation to perform for each matching object that could have changed
		 */
		void triggerAction(@Nullable R priorRoot, R newRoot, Reference<?> target, HookAction<S> action) throws InterruptedException {
			if (matchesTarget(target)) {
				triggerCascade(boundScopeFor(target), priorRoot, newRoot, action);
			}
		}

		/**
		 * @return this hook's scope bound to the path of a target that {@link #matchesTarget matches}
		 */
		private Reference<S> boundScopeFor(Reference<?> target) {
			assert matchesTarget(target);
			int relativeDepth = target.path().length() - scope.path().length();
			return relativeDepth >= 0
				? scope.boundBy(target.path().truncatedBy(relativeDepth))
				: scope.boundBy(target.path());
		}

		String name() {
			return this.name;
		}

		Reference<S> scope() {
			return this.scope;
		}

		BoskHook<S> hook() {
			return this.hook;
		}

		int index() {
			return this.index;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			@SuppressWarnings("unchecked")
			HookRegistration<S> that = (HookRegistration<S>) o;
			return Objects.equals(name, that.name)
				&& Objects.equals(scope, that.scope)
				&& Objects.equals(hook, that.hook);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, scope, hook);
		}

		@Override
		public String toString() {
			return "HookRegistration(name=" + this.name() + ", scope=" + this.scope() + ", hook=" + this.hook + ")";
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(Bosk.HOOK_LOGGER_NAME);
}
