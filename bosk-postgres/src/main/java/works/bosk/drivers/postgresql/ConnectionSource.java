package works.bosk.drivers.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Simplified lambda-friendly version of {@link javax.sql.DataSource}.
 */
public interface ConnectionSource {
	Connection getConnection() throws SQLException;

	public static ConnectionSource from(DataSource ds) {
		return ds::getConnection;
	}
}
