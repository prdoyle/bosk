package works.bosk.drivers.mongo.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a deferred function on another thread,
 * accepting input from the calling thread and returning output.
 * Like a {@link java.util.concurrent.FutureTask} that accepts a parameter.
 * <p>
 * The calling thread calls {@link #call(Object)} to provide input and receive output.
 * The background thread calls {@link #run()} to wait for input, execute the function,
 * and provide output. In case of failure, the background thread may call
 * {@link #fail(Exception)} instead. In case the function should be skipped entirely
 * and the output provided directly, call {@link #complete(Object)}.
 */
final class RemoteCallable<I, O> {
	private final Function<? super I, ? extends O> function;
	private final CompletableFuture<I> input = new CompletableFuture<>();
	private final CompletableFuture<O> output = new CompletableFuture<>();

	RemoteCallable(Function<? super I, ? extends O> function) {
		this.function = function;
	}

	/**
	 * Called by the main thread to provide input and wait for output.
	 */
	O call(I inputValue) throws InterruptedException, ExecutionException {
		input.complete(inputValue);
		return output.get();
	}

	/**
	 * Called by the background thread to wait for input, execute the function,
	 * and complete the output.
	 * <p>
	 * Handles the two exceptions that arise purely from the {@link #input}/{@link #output}
	 * future lifecycle, because their resolution is known without any caller policy:
	 * <ul>
	 *     <li>{@link ExecutionException} can only mean {@link #fail} completed {@link #input}
	 *     exceptionally, and {@link #fail} always settles {@link #output} first, so there is
	 *     nothing left to do.</li>
	 *     <li>{@link InterruptedException} means the thread was asked to stop before {@link #input}
	 *     arrived; we settle {@link #output} exceptionally so the calling thread isn't left waiting,
	 *     and restore the interrupt flag.</li>
	 * </ul>
	 * A {@link RuntimeException} (or {@link Error}) thrown by the function is a business-logic
	 * failure whose resolution (retry, fallback, abort) depends on the caller, so it propagates
	 * out with {@link #output} left unresolved. The caller must then settle it via
	 * {@link #fail} or {@link #complete}.
	 */
	void run() {
		I inputValue;
		try {
			inputValue = input.get();
		} catch (InterruptedException e) {
			output.completeExceptionally(e);
			Thread.currentThread().interrupt();
			return;
		} catch (ExecutionException e) {
			LOGGER.debug("Task completed via fail() before run() could execute the function", e);
			assert isDone();
			return;
		}
		output.complete(function.apply(inputValue));
	}

	/**
	 * Called by the background thread to complete the output with a result
	 * without executing the function.
	 */
	void complete(O result) {
		output.complete(result);
	}

	/**
	 * Called by the background thread to signal failure without executing the function.
	 */
	void fail(Exception e) {
		output.completeExceptionally(e);
		input.completeExceptionally(e);
	}

	boolean isDone() {
		return output.isDone();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(RemoteCallable.class);
}
