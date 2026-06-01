package works.bosk;

/**
 * Called to indicate the hook's "scope object" may have been modified.
 */
public interface BoskHook<T> {
	/**
	 * @param reference points to an object that may have been modified, corresponding
	 * to the scope on which this hook was registered. The referenced object may or
	 * may not exist.
	 * @throws InterruptedException as a convenience for implementations:
	 * bosk catches the exception, tidies up, and proceeds with the next hook.
	 */
	void onChanged(Reference<T> reference) throws InterruptedException;
}
