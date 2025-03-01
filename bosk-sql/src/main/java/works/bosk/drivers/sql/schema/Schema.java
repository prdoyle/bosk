package works.bosk.drivers.sql.schema;

import org.jooq.Name;
import org.jooq.Record;
import org.jooq.TableField;
import org.jooq.impl.SchemaImpl;

public class Schema extends SchemaImpl {
	public final BoskTable BOSK = new BoskTable(this);
	public final ChangesTable CHANGES = new ChangesTable(this);

	// Unqualified field names for convenience

	public final TableField<Record, String> ID = BOSK.ID;
	public final TableField<Record, String> STATE = BOSK.STATE;
	public final TableField<Record, Long>   REVISION = CHANGES.REVISION;
	public final TableField<Record, String> REF = CHANGES.REF;
	public final TableField<Record, String> NEW_STATE = CHANGES.NEW_STATE;
	public final TableField<Record, String> DIAGNOSTICS = CHANGES.DIAGNOSTICS;

	public Schema(Name name) {
		super(name);
	}
}
