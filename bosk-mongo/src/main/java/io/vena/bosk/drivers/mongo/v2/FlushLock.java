package io.vena.bosk.drivers.mongo.v2;

import io.vena.bosk.drivers.mongo.MongoDriverSettings;
import io.vena.bosk.exceptions.FlushFailureException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.bson.BsonInt64;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implements waiting mechanism for revision numbers
 */
@RequiredArgsConstructor
class FlushLock {
	private final MongoDriverSettings settings;
	private final PriorityBlockingQueue<Waiter> queue = new PriorityBlockingQueue<>();
	private volatile long alreadySeen = -1;

	@Value
	private static class Waiter implements Comparable<Waiter> {
		long revision;
		Semaphore semaphore;

		@Override
		public int compareTo(Waiter other) {
			return Long.compare(revision, other.revision);
		}
	}

	void awaitRevision(BsonInt64 revision) throws InterruptedException, FlushFailureException {
		if (revision.longValue() <= alreadySeen) {
			// Don't wait for revisions in the past
			return;
		}
		Semaphore semaphore = new Semaphore(0);
		queue.add(new Waiter(revision.longValue(), semaphore));
		if (!semaphore.tryAcquire(settings.flushTimeoutMS(), MILLISECONDS)) {
			throw new FlushFailureException("Timed out waiting for revision " + revision);
		}
	}

	/**
	 * @param revision can be null
	 */
	void notifyRevision(BsonInt64 revision) {
		if (revision == null) {
			return;
		}
		long revisionValue = revision.longValue();
		assert alreadySeen <= revisionValue;
		alreadySeen = revisionValue;
		do {
			Waiter w = queue.peek();
			if (w == null || w.revision > revisionValue) {
				return;
			} else {
				Waiter removed = queue.remove();
				assert w == removed;
				w.semaphore.release();
			}
		} while (true);
	}
}
