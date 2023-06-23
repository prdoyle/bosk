package io.vena.bosk.drivers.mongo.v3;

import io.vena.bosk.drivers.mongo.MongoDriverSettings;
import io.vena.bosk.drivers.mongo.v2.DisconnectedException;
import io.vena.bosk.exceptions.FlushFailureException;
import java.io.Closeable;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Value;
import org.bson.BsonInt64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.System.identityHashCode;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implements waiting mechanism for revision numbers
 */
class FlushLock implements Closeable {
	private final MongoDriverSettings settings;
	private final Lock queueLock = new ReentrantLock();
	private final PriorityBlockingQueue<Waiter> queue = new PriorityBlockingQueue<>();
	private volatile long alreadySeen;
	private boolean isClosed;

	/**
	 * @param revisionAlreadySeen needs to be the exact revision from the database:
	 * too old, and we'll wait forever for intervening revisions that have already happened;
	 * too new, and we'll proceed immediately without waiting for revisions that haven't happened yet.
	 */
	public FlushLock(MongoDriverSettings settings, long revisionAlreadySeen) {
		LOGGER.debug("New flush lock at revision {} [{}]", revisionAlreadySeen, identityHashCode(this));
		this.settings = settings;
		this.alreadySeen = revisionAlreadySeen;
	}

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
		long revisionValue = revision.longValue();
		Semaphore semaphore = new Semaphore(0);
		long past;
		try {
			queueLock.lock();
			if (isClosed) {
				throw new DisconnectedException("FlushLock is closed");
			}
			queue.add(new Waiter(revisionValue, semaphore));
			past = alreadySeen;
		} finally {
			queueLock.unlock();
		}
		if (revisionValue > past) {
			LOGGER.debug("Awaiting revision {} > {} [{}]", revisionValue, past, identityHashCode(this));
			if (!semaphore.tryAcquire(settings.flushTimeoutMS(), MILLISECONDS)) {
				throw new FlushFailureException("Timed out waiting for revision " + revisionValue + " > " + alreadySeen);
			}
			if (isClosed) {
				// Can't simply return and pretend this worked
				throw new DisconnectedException("FlushLock was closed while waiting");
			}
			LOGGER.debug("Done awaiting revision {} [{}]", revisionValue, identityHashCode(this));
		} else {
			LOGGER.debug("Revision {} <= {} is in the past; don't wait [{}]", revisionValue, past, identityHashCode(this));
		}
	}

	/**
	 * Called after updates are sent downstream.
	 * @param revision can be null
	 */
	void finishedRevision(BsonInt64 revision) {
		if (revision == null) {
			return;
		}

		try {
			queueLock.lock();
			long revisionValue = revision.longValue();
			if (isClosed) {
				LOGGER.debug("Closed FlushLock ignoring revision {} [{}]", revisionValue, identityHashCode(this));
				return;
			}
			if (revisionValue <= alreadySeen) {
				LOGGER.debug("Note: revision did not advance: {} <= {} [{}]", revisionValue, alreadySeen, identityHashCode(this));
			}

			do {
				Waiter w = queue.peek();
				if (w == null || w.revision > revisionValue) {
					break;
				} else {
					Waiter removed = queue.remove();
					assert w == removed;
					w.semaphore.release();
				}
			} while (true);

			alreadySeen = revisionValue;
			LOGGER.debug("Finished {} [{}]", revisionValue, identityHashCode(this));
		} finally {
			queueLock.unlock();
		}
	}

	@Override
	public void close() {
		try {
			queueLock.lock();
			LOGGER.debug("Closing [{}]", identityHashCode(this));
			isClosed = true;
			Waiter w;
			while ((w = queue.poll()) != null) {
				w.semaphore.release();
			}
		} finally {
			queueLock.unlock();
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(FlushLock.class);
}
