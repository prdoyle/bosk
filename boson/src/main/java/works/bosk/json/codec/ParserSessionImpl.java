package works.bosk.json.codec;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.json.mapping.Token;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.mapping.spec.PrimitiveNumberNode;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused")
public class ParserSessionImpl {
	protected final JsonReader input;

	public ParserSessionImpl(JsonReader input) {
		this.input = requireNonNull(input);
	}

	protected boolean parseBoolean() {
		Token token = input.peekToken();
		skipToken(token);
		return switch (token) {
			case FALSE -> false;
			case TRUE -> true;
			default -> throw new IllegalStateException("Expected boolean, not " + token);
		};
	}

	protected Number parseBigNumber() {
		logEntry("parseBigNumber");
		var token = input.peekToken();
		assert token == Token.NUMBER;
		return new BigDecimal(input.consumeNumber().toString());
	}

	protected Enum<?> parseEnumByName(MethodHandle valueOfHandle) {
		String name = input.consumeString();
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

	protected Object parsePrimitiveNumber(MethodHandle parseHandle) {
		var token = input.peekToken();
		assert token == Token.NUMBER;
		String string = input.consumeNumber().toString();
		try {
			return parseHandle.invoke(string);
		} catch (Throwable e) {
			throw new IllegalStateException("Error decoding number", e.getCause());
		}
	}

	protected CharSequence readNumber() {
		Token token = input.peekToken();
		if (token != Token.NUMBER) {
			parseError("Expected number, not " + token);
		}
		return input.consumeNumber();
	}

	protected int peekTokenOrdinal() {
		Token token = input.peekToken();
		LOGGER.debug("peekTokenOrdinal: {}", token);
		return token.ordinal();
	}

	/**
	 * Utility method so you can toss code in here and see what its bytecode looks like
	 */
	private void decomp(Map<?,?> map) {
//		skip(-1);
//		map.put(parseEnumByName(null), new BigDecimal("0"));
	}

	protected String parseString() {
		var token = input.peekToken();
		assert token == Token.STRING;
		return input.consumeString();
	}

	protected JsonStringCharacterReader processString() {
		return input.processString();
	}

	protected String previewString() {
		if (true) {//(LOGGER.isDebugEnabled()) {
			return input.previewString(10)
				.replace('\n', ' ')
				.replace('\r', ' ');
		} else {
			return "⁇";
		}
	}

	protected void skipTokenWithOrdinal(int ord) {
		Token token = Token.values()[ord];
		LOGGER.debug("skipTokenWithOrdinal: {}", token);
		skipToken(token);
	}

	protected void skipToken(Token readToken) {
		var token = input.peekToken();
		if (token != readToken) {
			parseError("Expected token " + readToken + ", not " + token);
		}
		input.consumeFixedToken(readToken);
	}

	protected String readString(int firstChar) {
		return input.consumeString();
	}

	protected void parseError(String message) {
		String previewString = previewString();
		throw new IllegalStateException(message + " at offset " + input.currentOffset() + ": |" + previewString + "|");
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
			LOGGER.trace("{} @ {}: |{}|", methodName, input.currentOffset(), previewString());
		}
	}
	
	protected void logEntry(String methodName, Object arg) {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("{}({}) @ {}: |{}|", methodName, arg, input.currentOffset(), previewString());
		}
		assert !(arg instanceof JsonValueSpec): "Why are we passing SpecNodes here instead of in the interpreter?";
	}
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ParserSessionImpl.class);

}
