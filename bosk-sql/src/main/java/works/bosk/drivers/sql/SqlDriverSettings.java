package works.bosk.drivers.sql;

import static java.lang.Math.multiplyExact;

/**
 * @param timescaleMS how often the system should check for updates events in the database.
 *                          Other timeout settings are scaled relative to this as appropriate.
 * @param patienceFactor a multiplier on {@code timescaleMS} indicating how long the system should wait when something has gone wrong.
 * @param retries how many times a failed operation should be attempted after the first failure.
 *                A value of 0 means operations will not be retried at all.
 *                Also used as a general indicator of how "persistent" the system ought to be before reporting an error to the user.
 */
public record SqlDriverSettings(
	long timescaleMS,
	int patienceFactor,
	int retries
) {
	public SqlDriverSettings {
		multiplyExact(timescaleMS, patienceFactor);
	}
}
