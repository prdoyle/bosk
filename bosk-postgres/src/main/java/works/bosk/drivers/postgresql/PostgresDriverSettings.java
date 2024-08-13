package works.bosk.drivers.postgresql;

public record PostgresDriverSettings(
	String url, // Without the database
	String database,
	String schema
) { }
