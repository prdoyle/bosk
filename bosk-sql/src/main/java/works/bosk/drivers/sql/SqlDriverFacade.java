package works.bosk.drivers.sql;

import lombok.experimental.Delegate;

/**
 * The actual object a user interacts with.
 * Calls from users go through this object,
 * while internal calls can go through {@link SqlDriverImpl}
 * and skip certain logic that isn't needed for internal calls.
 */
class SqlDriverFacade implements SqlDriver {
	@Delegate final SqlDriverImpl impl;

	SqlDriverFacade(SqlDriverImpl impl) {
		this.impl = impl;
	}
}
