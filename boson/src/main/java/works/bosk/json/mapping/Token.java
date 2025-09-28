package works.bosk.json.mapping;

public enum Token {
	START_TEXT,
	END_TEXT,
	NULL,
	FALSE,
	TRUE,
	NUMBER,
	START_OBJECT,
	END_OBJECT,
	START_ARRAY,
	END_ARRAY,

	/**
	 * Can be a member name or a string value.
	 * We don't distinguish at the token level.
	 */
	STRING,

	/**
	 * Includes whitespace and commas between meaningful tokens
	 */
	INSIGNIFICANT,

	ERROR;

	public static Token startingWith(int codePoint) {
		return switch (codePoint) {
			case -1 -> END_TEXT;
			case 'n', 'N' -> NULL;
			case 'f', 'F' -> FALSE;
			case 't', 'T' -> TRUE;
			case '1', '2', '3', '4', '5', '6', '7', '8', '9', '-' -> NUMBER;
			case '{' -> START_OBJECT;
			case '}' -> END_OBJECT;
			case '[' -> START_ARRAY;
			case ']' -> END_ARRAY;
			case '"' -> STRING;
			case 0x20, 0x0A, 0x0D, 0x09, ',', ':' -> INSIGNIFICANT;
			default -> ERROR;
		};
	}

	public String fixedRepresentation() {
		return switch (this) {
			case START_TEXT, END_TEXT -> "";
			case NULL -> "null";
			case FALSE -> "false";
			case TRUE -> "true";
			case START_OBJECT -> "{";
			case END_OBJECT -> "}";
			case START_ARRAY -> "[";
			case END_ARRAY -> "]";
			default ->
				throw new IllegalArgumentException("Token has no fixed representation: " + this);
		};
	}

}
