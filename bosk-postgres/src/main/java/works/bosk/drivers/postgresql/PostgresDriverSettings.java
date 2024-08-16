package works.bosk.drivers.postgresql;

public record PostgresDriverSettings(
	String url, // Without the database
	String database
) {
	@Override
	public String toString() {
		return "PostgresDriverSettings[" +
			"url=" + url + ", " +
			"database=" + database + ']';
	}
}
