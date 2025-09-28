package works.bosk.json.codec;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.json.mapping.Token;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.mapping.spec.PrimitiveNumberNode;

import static java.util.Objects.requireNonNull;
import static works.bosk.json.mapping.Token.INSIGNIFICANT;
import static works.bosk.json.mapping.Token.NUMBER;
import static works.bosk.json.mapping.Token.STRING;

@SuppressWarnings("unused")
public class ParserSessionImpl {
	protected final CharArrayReader input;

	public ParserSessionImpl(CharArrayReader input) {
		this.input = requireNonNull(input);
	}

	protected boolean parseBoolean() throws IOException {
		Token token = nextToken();
		skipToken(token);
		return switch (token) {
			case FALSE -> false;
			case TRUE -> true;
			default -> throw new IllegalStateException("Expected boolean, not " + token);
		};
	}

	protected Number parseBigNumber() throws IOException {
		logEntry("parseBigNumber");
		return new BigDecimal(readNumber(nextSignificant()).toString());
	}

	protected Enum<?> parseEnumByName(MethodHandle valueOfHandle) throws IOException {
		String name = readString(nextSignificant());
		Enum<?> result;
		try {
			result = (Enum<?>) valueOfHandle.invoke(name);
		} catch (Throwable e) {
			throw new IllegalStateException("Error decoding enum name", e.getCause());
		}
		if (result == null) {
			throw new IllegalStateException("No enum constant " + valueOfHandle.type().returnType().getSimpleName() + "." + name);
		}
		return result;
	}

	protected Object parsePrimitiveNumber(MethodHandle parseHandle) throws IOException {
		String string = readNumber(nextSignificant()).toString();
		try {
			return parseHandle.invoke(string);
		} catch (Throwable e) {
			throw new IllegalStateException("Error decoding number", e.getCause());
		}
	}

	/**
	 * Utility method so you can toss code in here and see what its bytecode looks like
	 */
	private void decomp(Map<?,?> map) throws Exception {
//		skip(-1);
//		map.put(parseEnumByName(null), new BigDecimal("0"));
	}

	protected String parseString() throws IOException {
		logEntry("parseString");
		return readString(nextSignificant());
	}

	protected String previewString() {
		if (LOGGER.isTraceEnabled()) {
			return input.previewString(10)
				.replace('\n', ' ')
				.replace('\r', ' ');
		} else {
			return "";
		}
	}

	/**
	 * Parsing numbers is a pain in the ass for many reasons.
	 * Just get the digits into a {@link CharSequence} and get on with life.
	 */
	protected StringBuilder readNumber(int firstChar) throws IOException {
		assert Token.startingWith(firstChar) == NUMBER;
		StringBuilder sb = new StringBuilder();
		sb.appendCodePoint(firstChar);
		for (int c = read(); ; c = read()) {
			switch (c) {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
					 '-', '.', 'e', 'E' -> sb.appendCodePoint(c);
				default -> {
					// And suddenly, without warning, we have walked off
					// the end of the number.
					input.skip(-1);
					return sb;
				}
			}
		}
	}

	protected int read() throws IOException {
		return input.read();
	}

	protected void skip(int offset) {
		input.skip(offset);
	}

	/**
	 * When positioned at either the start of a token or an {@link Token#INSIGNIFICANT},
	 * advance to the next character that is not insignificant and return its token.
	 */
	protected Token nextToken() throws IOException {
		Token result;
		do {
			int read = read();
			result = Token.startingWith(read);
		} while (result == INSIGNIFICANT);
		return result;
	}

	protected int nextSignificant() throws IOException {
		int result;
		do {
			result = read();
		} while (fast_isInsignificant(result));
		logEntry("nextSignificant");
		return result;
	}

	static boolean isInsignificant(int codePoint) {
		boolean result = switch(codePoint) {
			// Binary:
			// 0010 0000
			// 0000 1010
			// 0000 1101
			// 0010 1100
			// 0011 1010
			case 0x20, 0x0A, 0x0D, 0x09, ',', ':' -> true;
			default -> false;
		};
		assert result == (Token.startingWith(codePoint) == INSIGNIFICANT);
		return result;
	}

	/**
	 * A branch-free version of {@link #isInsignificant}.
	 */
	static boolean fast_isInsignificant(int codePoint) {
		// The position to check in INSIGNIFICANT_CHARS
		long bit = 1L << codePoint;

		// Zero if definitely significant
		// Can have false positives
		long bitIsSet = INSIGNIFICANT_CHARS & bit;

		// All ones if codePoint is greater than the largest insignificant char
		long isNegative = (long)codePoint >> 63;
		long isTooBig = (63L - codePoint) >> 63;

		// Zero if significant
		long answer = bitIsSet & ~(isNegative | isTooBig);

		boolean result = (answer != 0);
		assert result == (Token.startingWith(codePoint) == INSIGNIFICANT);
		return result;
	}

	private static final long INSIGNIFICANT_CHARS = LongStream
		.of(0x20, 0x0A, 0x0D, 0x09, ',', ':')
		.map(n -> 1L << n)
		.sum();

	protected void skipTokenWithOrdinal(int ord) throws IOException {
		skipToken(Token.values()[ord]);
	}

	/**
	 * Assumes the first character has already been consumed while
	 * identifying the token.
	 */
	protected void skipToken(Token readToken) throws IOException {
		input.skip(readToken.fixedRepresentation().length() - 1);
	}

	protected String readString(int firstChar) throws IOException {
		logEntry("readString");
		assert Token.startingWith(firstChar) == STRING: "Expected quote, found [" + Character.toString(firstChar) + "]";
		StringBuilder sb = new StringBuilder();
		int c;
		while ((c = read()) != '"') {
			if (c == '\\') {
				int escapeCode = read();
				switch (escapeCode) {
					case '"': sb.append('"'); break;
					case '\\': sb.append('\\'); break;
					case '/': sb.append('/'); break;
					case 'b': sb.append('\b'); break;
					case 'f': sb.append('\f'); break;
					case 'n': sb.append('\n'); break;
					case 'r': sb.append('\r'); break;
					case 't': sb.append('\t'); break;
					case 'u': {
						// Read 4 hex digits and parse using Integer.parseInt
						char[] hexChars = new char[4];
						for (int i = 0; i < 4; i++) {
							hexChars[i] = (char) read();
						}
						int codePoint = Integer.parseInt(new String(hexChars), 16);
						sb.append((char) codePoint);
						break;
					}
					default:
						throw new IllegalStateException("Invalid escape character: \\" + (char)escapeCode);
				}
			} else {
				sb.append((char) c);
			}
		}
		return sb.toString();
	}

	/**
	 * Usually for member names
	 */
	protected void skipRemainderOfString(int remainingLength) throws IOException {
		input.skip(remainingLength);

		// At this point, the next character should be the close-quote,
		// unless the string contained some escaped characters, in which
		// case we've undercounted a bit.
		//
		// We're cheating a little here. If the last character in the string
		// happens to be an escaped quote, then we will incorrectly interpret
		// that as the close-quote. However, this is currently impossible in
		// practice: skipping only happens when we know the rest of the string,
		// which only happens for record components, whose names are constrained
		// by the Java naming conventions, which forbid quotes.

		int c;
		while ((c= read()) != '"') {
			if (c == '\\') {
				input.skip(1);
			}
		}
	}

	protected void parseError(String message) {
		input.skip(-1); // SHOULDN'T HAVE A SIDE EFFECT!
		String previewString = previewString();
		input.skip(1);
		throw new IllegalStateException(message + " at offset " + input.offset() + ": |" + previewString + "|");
	}

	private static final Map<Class<?>, MethodHandle> PRIMITIVE_PARSE_HANDLES = new ConcurrentHashMap<>();

	public static final Map<Class<?>, String> PRIMITIVE_PARSE_METHOD_NAMES = Map.of(
		byte.class, "parseByte",
		short.class, "parseShort",
		int.class, "parseInt",
		long.class, "parseLong",
		float.class, "parseFloat",
		double.class, "parseDouble");

	public static MethodHandle parseHandle(Class<?> primitiveType) {
		assert primitiveType.isPrimitive();
		return PRIMITIVE_PARSE_HANDLES.computeIfAbsent(primitiveType, t -> {
			var methodName = PRIMITIVE_PARSE_METHOD_NAMES.get(t);
			try {
				var method = PrimitiveNumberNode.PRIMITIVE_NUMBER_CLASSES.get(t).getMethod(methodName, String.class);
				return MethodHandles.lookup().unreflect(method);
			} catch (NoSuchMethodException | IllegalAccessException e) {
				throw new IllegalStateException(e);
			}
		});
	}

	private static final ConcurrentHashMap<Class<?>, MethodHandle> VALUE_OF_HANDLES = new ConcurrentHashMap<>();

	public static MethodHandle valueOfHandle(Class<?> clazz) {
		return VALUE_OF_HANDLES.computeIfAbsent(clazz, ParserSessionImpl::computeValueOfHandle);
	}

	private static MethodHandle computeValueOfHandle(Class<?> boxedType) {
		try {
			Method valueOf = boxedType.getMethod("valueOf", String.class);
			return MethodHandles.lookup().unreflect(valueOf);
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new IllegalStateException("Unexpected error looking up valueOf for " + boxedType.getSimpleName(), e);
		}
	}

	protected void logEntry(String methodName) {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("{} @ {}: |{}|", methodName, input.offset(), previewString());
		}
	}
	
	protected void logEntry(String methodName, Object arg) {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("{}({}) @ {}: |{}|", methodName, arg, input.offset(), previewString());
		}
		assert !(arg instanceof JsonValueSpec): "Why are we passing SpecNodes here instead of in the interpreter?";
	}
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ParserSessionImpl.class);

}
