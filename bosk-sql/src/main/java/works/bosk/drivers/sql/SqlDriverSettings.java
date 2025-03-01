package works.bosk.drivers.sql;


import org.jooq.CheckReturnValue;

import static java.lang.Math.multiplyExact;

/**
 * @param timescaleMS    how often the system should check for updates events in the database.
 *                       Characterizes the "response time" of the system: lower values
 *                       are more responsive, while higher values are more efficient.
 *                       Other timeout settings are scaled relative to this as appropriate.
 * @param patienceFactor a multiplier on {@code timescaleMS} indicating how long the system
 *                       should wait before giving up when something has gone wrong.
 *                       Short times can make errors visible to the application
 *                       in cases where waiting longer would have allowed the system to recover;
 *                       long times can make an application appear to hang when it would have
 *                       been better off giving up and proceeding with error handling.
 */
public record SqlDriverSettings(
	long timescaleMS,
	int patienceFactor
) {
	public SqlDriverSettings {
		multiplyExact(timescaleMS, patienceFactor);
	}
}
