package works.bosk.drivers.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;
import works.bosk.DriverFactory;
import works.bosk.StateTreeNode;

public interface SqlDriver extends BoskDriver {
	static <RR extends StateTreeNode> SqlDriverFactory<RR> factory(
		SqlDriverSettings settings,
		ConnectionSource connectionSource,
		Function<BoskInfo<RR>, ObjectMapper> objectMapperFactory
	) {
		return (b, d) -> new SqlDriverFacade(new SqlDriverImpl(
			settings, connectionSource, b, objectMapperFactory.apply(b), d
		));
	}

	void close();

	interface ConnectionSource {
		Connection get() throws SQLException;
	}

	interface SqlDriverFactory<RR extends StateTreeNode> extends DriverFactory<RR> {
		@Override SqlDriver build(BoskInfo<RR> boskInfo, BoskDriver downstream);
	}
}
