package works.bosk.drivers.sql.schema;

import org.jooq.Record;
import org.jooq.TableField;

import static org.jooq.impl.SQLDataType.BIGINT;
import static org.jooq.impl.SQLDataType.CLOB;
import static org.jooq.impl.SQLDataType.VARCHAR;

public class Schema {
	public static final BoskTable BOSK = new BoskTable();
	public static final ChangesTable CHANGES = new ChangesTable();

	// Unqualified field names for convenience

	public static final TableField<Record, String> ID = BOSK.ID;
	public static final TableField<Record, String> STATE = BOSK.STATE;
	public static final TableField<Record, Long> REVISION = CHANGES.REVISION;
	public static final TableField<Record, String> REF = CHANGES.REF;
	public static final TableField<Record, String> NEW_STATE = CHANGES.NEW_STATE;
	public static final TableField<Record, String> DIAGONSTICS = CHANGES.DIAGONSTICS;

	private Schema() {}
}
