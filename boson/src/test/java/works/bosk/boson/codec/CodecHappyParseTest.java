package works.bosk.boson.codec;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import works.bosk.boson.codec.PrimitiveInjector.PrimitiveNumber;
import works.bosk.boson.mapping.TypeMap.Settings;
import works.bosk.boson.mapping.TypeScanner;
import works.bosk.boson.mapping.spec.ArrayNode;
import works.bosk.boson.mapping.spec.BigNumberNode;
import works.bosk.boson.mapping.spec.BooleanNode;
import works.bosk.boson.mapping.spec.BoxedPrimitiveSpec;
import works.bosk.boson.mapping.spec.ComputedSpec;
import works.bosk.boson.mapping.spec.EnumByNameNode;
import works.bosk.boson.mapping.spec.FixedObjectNode;
import works.bosk.boson.mapping.spec.MaybeAbsentSpec;
import works.bosk.boson.mapping.spec.MaybeNullSpec;
import works.bosk.boson.mapping.spec.ParseCallbackSpec;
import works.bosk.boson.mapping.spec.PrimitiveNumberNode;
import works.bosk.boson.mapping.spec.RecognizedMember;
import works.bosk.boson.mapping.spec.RepresentAsSpec;
import works.bosk.boson.mapping.spec.StringNode;
import works.bosk.boson.mapping.spec.UniformMapNode;
import works.bosk.boson.mapping.spec.UniformMapNode.MemberValueWrangler;
import works.bosk.boson.mapping.spec.handles.ArrayAccumulator;
import works.bosk.boson.mapping.spec.handles.ArrayEmitter;
import works.bosk.boson.mapping.spec.handles.MemberPresenceCondition;
import works.bosk.boson.mapping.spec.handles.ObjectAccumulator;
import works.bosk.boson.mapping.spec.handles.ObjectAccumulator.KeyHandlingWrangler;
import works.bosk.boson.mapping.spec.handles.ObjectEmitter;
import works.bosk.boson.mapping.spec.handles.TypedHandle;
import works.bosk.boson.mapping.spec.handles.TypedHandles;
import works.bosk.boson.types.BoundType;
import works.bosk.boson.types.DataType;
import works.bosk.boson.types.KnownType;
import works.bosk.boson.types.TypeReference;
import works.bosk.junit.InjectFields;
import works.bosk.junit.InjectFrom;
import works.bosk.junit.Injected;
import works.bosk.junit.InjectedTest;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.boson.types.DataType.BOOLEAN;
import static works.bosk.boson.types.DataType.INT;
import static works.bosk.boson.types.DataType.STRING;

/**
 * Tests that {@link Codec} parses valid JSON correctly.
 */
@InjectFields
@InjectFrom({PrimitiveInjector.class, SettingsInjector.class})
public class CodecHappyParseTest {
	@Injected
	Settings settings;

	TypeScanner scanner;

	@BeforeEach
	void setup() {
		scanner = new TypeScanner(settings).useLookup(MethodHandles.lookup());
	}

	static List<Settings> settings() {
		return new SettingsInjector().values();
	}

	@Test
	void bareLiterals() throws IOException {
		var typeMap = scanner
			.scan(BOOLEAN)
			.scan(STRING)
			.build();
		var codec = CodecBuilder.using(typeMap).build();
		assertEquals(false,
			codec.parserFor(new BooleanNode()).parse(JsonReader.create("false")));
		assertEquals(true,
			codec.parserFor(new BooleanNode()).parse(JsonReader.create("true")));
		assertNull(codec.parserFor(new MaybeNullSpec(new StringNode())).parse(JsonReader.create("null")));
	}

	@Test
	void bareString() throws IOException {
		var typeMap = scanner
			.scan(STRING)
			.build();
		var codec = CodecBuilder.using(typeMap).build();
		String everyAsciiCharacter = IntStream.range(32, 127)
			.filter(i -> i != '"' && i != '\\') // Exclude " and \
			.mapToObj(i -> String.valueOf((char) i))
			.reduce("", (a, b) -> a + b);
		assertEquals(everyAsciiCharacter,
			codec.parserFor(new StringNode()).parse(JsonReader.create("\"" + everyAsciiCharacter +"\"")));
	}

	@InjectedTest
	void primitiveNumber(PrimitiveNumber numberCase) throws IOException {
		var typeMap = scanner
			.scan(DataType.of(numberCase.type()))
			.build();
		var codec = CodecBuilder.using(typeMap).build();
		assertEquals(numberCase.value(),
			codec
				.parserFor(new PrimitiveNumberNode(numberCase.type()))
				.parse(JsonReader.create(numberCase.json())));
	}

	@InjectedTest
	void boxedNumber(PrimitiveNumber numberCase) throws IOException {
		var typeMap = scanner
			.scan(DataType.of(numberCase.boxedType()))
			.build();
		var codec = CodecBuilder.using(typeMap).build();
		BoxedPrimitiveSpec spec = new BoxedPrimitiveSpec(new PrimitiveNumberNode(numberCase.type()));
		assertEquals(numberCase.value(),
			codec.parserFor(spec)
				.parse(JsonReader.create(numberCase.json())));
		assertNull(codec.parserFor(new MaybeNullSpec(spec)).parse(JsonReader.create("null")));
	}

	@Test
	void bigDecimal() throws IOException {
		var typeMap = scanner
			.scan(DataType.of(BigDecimal.class))
			.build();
		var codec = CodecBuilder.using(typeMap).build();
		var pi = "3.141592653589793238462643383279502884197169399375105820974944";
		assertEquals(new BigDecimal(pi),
			codec.parserFor(new BigNumberNode(BigDecimal.class)).parse(JsonReader.create(pi)));
	}

	@Test
	void enumByName() throws IOException {
		var typeMap = scanner
			.scan(DataType.of(TimeUnit.class))
			.build();
		var codec = CodecBuilder.using(typeMap).build();
		assertEquals(TimeUnit.DAYS,
			codec.parserFor(new EnumByNameNode(TimeUnit.class)).parse(JsonReader.create("\"DAYS\"")));
	}

	/**
	 * As a demonstration of flexibility, we represent the array as a pipe-separated string,
	 * rather than the more obvious List<String>.
	 */
	@Test
	void array() throws IOException {
		var spec = new ArrayNode(
			new StringNode(),
			ArrayAccumulator.from(new ArrayAccumulator.Wrangler<StringBuilder, String, String>() {
				@Override
				public StringBuilder create() {
					return new StringBuilder();
				}

				@Override
				public StringBuilder integrate(StringBuilder accumulator, String element) {
					if (!accumulator.isEmpty()) {
						accumulator.append('|');
					}
					accumulator.append(element);
					return accumulator;
				}

				@Override
				public String finish(StringBuilder accumulator) {
					return accumulator.toString();
				}
			}),
			ArrayEmitter.from(new ArrayEmitter.Wrangler<String, Iterator<String>, String>() {
				@Override
				public Iterator<String> start(String representation) {
					String[] parts = representation.split("\\|", -1);
					return List.of(parts).iterator();
				}

				@Override
				public boolean hasNext(Iterator<String> iterator) {
					return iterator.hasNext();
				}

				@Override
				public String next(Iterator<String> iterator) {
					return iterator.next();
				}
			})
		);
		var json = """
			[ "first", "second", "third" ]
			""";
		var codec = CodecBuilder
			.using(scanner.scan(STRING).build())
			.build(spec);
		assertEquals("first|second|third",
			codec.parserFor(spec).parse(JsonReader.create(json)));
	}

	/**
	 * As a demonstration of flexibility, we represent the map as a colon-separated string,
	 * rather than the more obvious record or map.
	 */
	@Test
	void fixedObject() throws IOException {
		var memberSpecs = new LinkedHashMap<String, RecognizedMember>();
		memberSpecs.put("intField",
			new RecognizedMember(
				new PrimitiveNumberNode(int.class),
				TypedHandles.<String,Integer>function(STRING, INT, s ->
					Integer.parseInt(s.split(":", 2)[0]))
			)
		);
		memberSpecs.put("strField",
			new RecognizedMember(
				new StringNode(),
				TypedHandles.<String,String>function(STRING, STRING, s ->
					s.split(":", 2)[1])
			)
		);
		var spec = new FixedObjectNode(
			memberSpecs,
			TypedHandles.<Integer, String, String>biFunction(INT, STRING, STRING,
				(i, s) -> i + ":" + s
			)
		);

		var codec = CodecBuilder
			.using(scanner.scan(INT).scan(STRING).build())
			.build(spec);
		var json = """
			{
				"intField": 123,
				"strField": "Hello, World!"
			}
			""";
		assertEquals("123:Hello, World!",
			codec.parserFor(spec).parse(JsonReader.create(json)));
	}

	@Test
	void optionalField() throws IOException {
		record TestRecord(int intField, String strField) {}
		var optional = new RecognizedMember(
			new MaybeAbsentSpec(
				new StringNode(),
				new ComputedSpec(TypedHandles.supplier(STRING, () -> "TEST_DEFAULT")),
				MemberPresenceCondition.always()
			),
			TypedHandles.componentAccessor(TestRecord.class.getRecordComponents()[1], MethodHandles.lookup())
		);
		var typeMap = scanner
			.specifyRecordFields(TestRecord.class, Map.of("strField", optional))
			.scan(DataType.of(TestRecord.class))
			.build();
		var codec = CodecBuilder.using(typeMap).build();

		assertEquals(new TestRecord(123, "Hello, World!"),
			codec.parserFor(typeMap.get(DataType.of(TestRecord.class)))
				.parse(JsonReader.create("""
					{
						"intField": 123,
						"strField": "Hello, World!"
					}
					""")));
		assertEquals(new TestRecord(456, "TEST_DEFAULT"),
			codec.parserFor(typeMap.get(DataType.of(TestRecord.class)))
				.parse(JsonReader.create("""
					{
						"intField": 456
					}
					""")));
	}

	@Test
	void parseCallback() throws IOException {
		// Set up the callback
		record Event(String beforeResult, String parsedValue) {}
		var eventRecord = new ArrayList<Event>();
		TypedHandle before = TypedHandles.supplier(STRING, () ->
			"before result");
		TypedHandle after = TypedHandles.<String,String>biConsumer(STRING, STRING, (br, pv) ->
			eventRecord.add(new Event(br, pv)));
		var spec = new ParseCallbackSpec(before, new StringNode(), after);

		// Do the parsing
		var typeMap = scanner.scan(STRING).build();
		Codec codec = CodecBuilder.using(typeMap).build(spec);
		var actual = codec.parserFor(spec).parse(JsonReader.create("\"parsed value\""));
		assertEquals("parsed value", actual);

		// Make sure exactly the right callbacks happened
		assertEquals(List.of(new Event("before result", "parsed value")), eventRecord);
	}

	@Test
	void oneMemberWithNullKeyHandlerResult() throws IOException {
		// Using MemberValueWrangler.nop() which returns null from beforeValue.
		// The interpreter must pass this null to the integrator,
		// not skip it (which would cause a WrongMethodTypeException).
		record Result(String key, String value) {}

		var wrangler = new UniformMapNode.OneMemberWrangler<Result, String, String>() {
			@Override public String getKey(Result v) { return v.key(); }
			@Override public String getValue(Result v) { return v.value(); }
			@Override public Result finish(String key, String value) { return new Result(key, value); }
		};
		var spec = UniformMapNode.oneMember(wrangler, MemberValueWrangler.nop());
		var typeMap = scanner.scan(STRING).build();
		var codec = CodecBuilder.using(typeMap).build(spec);

		var actual = codec.parserFor(spec).parse(JsonReader.create("""
			{"hello": "world"}
			"""));
		assertEquals(new Result("hello", "world"), actual);
	}

	@Test
	void memberCallback() throws IOException {
		// Prepare to record calls
		record Call(String key, BigDecimal value, String context) {}
		var actualCalls = new ArrayList<Call>();

		var acc = ObjectAccumulator.from(
			new KeyHandlingWrangler<
							LinkedHashMap<String, BigDecimal>,
							LinkedHashMap<String, BigDecimal>,
							String, BigDecimal, String>() {
				@Override public LinkedHashMap<String, BigDecimal> create() { return new LinkedHashMap<>(); }
				@Override public String keyHandler(LinkedHashMap<String, BigDecimal> acc, String key) {
					return "context for " + key;
				}
				@Override public LinkedHashMap<String, BigDecimal> integrate(
					LinkedHashMap<String, BigDecimal> acc, String key, BigDecimal value, String handlerResult) {
					actualCalls.add(new Call(key, value, handlerResult));
					acc.put(key, value);
					return acc;
				}
				@Override public LinkedHashMap<String, BigDecimal> finish(LinkedHashMap<String, BigDecimal> acc) { return acc; }
			}
		);

		BoundType mapType = (BoundType) DataType.known(new TypeReference<LinkedHashMap<String, BigDecimal>>() { });
		var typeMap = scanner.scan(mapType).build();
		var spec = new UniformMapNode(
			new StringNode(),
			new BigNumberNode(BigDecimal.class),
			acc,
			TypeScanner.mapEmitter(mapType)
		);
		var codec = CodecBuilder.using(typeMap).build(spec);

		var json = """
			{
				"member1": 10,
				"member2": 20,
				"member3": 30
			}
			""";
		Map<?,?> actualValue = (Map<?, ?>) codec.parserFor(spec).parse(JsonReader.create(json));
		assertEquals(Map.of(
			"member1", new BigDecimal("10"),
			"member2", new BigDecimal("20"),
			"member3", new BigDecimal("30")
		), actualValue);
		assertEquals(List.of("member1", "member2", "member3"), List.copyOf(actualValue.keySet()),
			"Keys must be added in the correct order");

		assertEquals(List.of(
			new Call("member1", new BigDecimal("10"), "context for member1"),
			new Call("member2", new BigDecimal("20"), "context for member2"),
			new Call("member3", new BigDecimal("30"), "context for member3")
		), actualCalls);
	}

	@Test
	void representAs() throws IOException {
		record TestRecord(String value) {}
		var typeMap = scanner
			.specify(DataType.of(TestRecord.class), RepresentAsSpec.as(
				new StringNode(),
				DataType.known(TestRecord.class),
				TestRecord::value,
				TestRecord::new
			))
			.build();
		var codec = CodecBuilder.using(typeMap).build();
		assertEquals(new TestRecord("test value"),
			codec.parserFor(typeMap.get(DataType.of(TestRecord.class)))
				.parse(JsonReader.create("\"test value\"")));
	}

	@Test
	void uniformMapNode() throws IOException {
		BoundType mapType = (BoundType) DataType.known(new TypeReference<LinkedHashMap<String, BigDecimal>>() { });
		var spec = new UniformMapNode(
			new StringNode(),
			new BigNumberNode(BigDecimal.class),
			TypeScanner.mapAccumulator(mapType),
			TypeScanner.mapEmitter(mapType)
		);

		var typeMap = scanner
			.scan(STRING)
			.scan(DataType.of(BigDecimal.class))
			.build();
		var codec = CodecBuilder.using(typeMap).build(spec);
		var json = """
			{
				"member1": 10,
				"member2": 20,
				"member3": 30
			}
			""";
		var expected = new LinkedHashMap<String, BigDecimal>();
		expected.put("member1", new BigDecimal("10"));
		expected.put("member2", new BigDecimal("20"));
		expected.put("member3", new BigDecimal("30"));

		Map<?,?> actual = (Map<?, ?>) codec.parserFor(spec).parse(JsonReader.create(json));
		assertEquals(expected, actual);

		assertEquals(List.copyOf(expected.keySet()), List.copyOf(actual.keySet()),
			"Keys are in the expected order");
	}

	/**
	 * This is a silly test, but it demonstrates the expressiveness of UniformMapNode
	 * by directly summing the map values during parsing, entirely with primitives.
	 */
	@Test
	void primitiveUniformMapNode() throws IOException, NoSuchMethodException, IllegalAccessException {
		TypedHandle intIdentity = new TypedHandle(
			MethodHandles.identity(int.class),
			INT, List.of(INT)
		);
		var spec = new UniformMapNode(
			new StringNode(),
			new PrimitiveNumberNode(int.class),
			new ObjectAccumulator(
				TypedHandles.constant(INT, 0),
				TypedHandles.biConsumer(INT, STRING, (a, k) -> {}),
				new TypedHandle(
					MethodHandles.lookup().findStatic(
						CodecHappyParseTest.class,
						"sumIntegrator",
						MethodType.methodType(int.class, int.class, String.class, int.class)
					),
					INT, List.of(INT, STRING, INT)),
				intIdentity
			),
			// The emitter isn't used here, but it needs to be realistic enough
			// to satisfy the validation assertions.
			Unsummer.emitter()
		);

		var typeMap = scanner
			.scan(INT)
			.build();
		var codec = CodecBuilder.using(typeMap).build(spec);
		var json = """
			{
				"member1": 10,
				"member2": 20,
				"member3": 30
			}
			""";
		assertEquals(60, codec.parserFor(spec).parse(JsonReader.create(json)));
	}

	@Test
	void memberCallbackPrimitive() throws IOException, NoSuchMethodException, IllegalAccessException {
		memberCallbackAfterCalls.clear();
		memberCallbackBeforeCalls.clear();

		var keyHandler = new TypedHandle(
			MethodHandles.lookup().findStatic(CodecHappyParseTest.class, "memberCallbackBefore",
				MethodType.methodType(void.class, int.class, String.class)),
			DataType.VOID, List.of(INT, STRING));

		var spec = new UniformMapNode(
			new StringNode(),
			new PrimitiveNumberNode(int.class),
			new ObjectAccumulator(
				TypedHandles.constant(INT, 0),
				keyHandler,
				new TypedHandle(
					MethodHandles.lookup().findStatic(CodecHappyParseTest.class, "sumAndRecordIntegrator",
						MethodType.methodType(int.class, int.class, String.class, int.class)),
					INT, List.of(INT, STRING, INT)),
				TypedHandles.identity(INT)
			),
			Unsummer.emitter()
		);

		var typeMap = scanner
			.scan(INT)
			.build();
		var codec = CodecBuilder.using(typeMap).build(spec);
		var json = """
			{
				"member1": 10,
				"member2": 20,
				"member3": 30
			}
			""";
		assertEquals(60, codec.parserFor(spec).parse(JsonReader.create(json)));

		assertEquals(List.of("member1", "member2", "member3"), memberCallbackBeforeCalls);
		assertEquals(List.of(
			Map.entry("member1", 10),
			Map.entry("member2", 20),
			Map.entry("member3", 30)
		), memberCallbackAfterCalls);
	}

	static final List<String> memberCallbackBeforeCalls = new ArrayList<>();

	static void memberCallbackBefore(int accumulator, String key) {
		memberCallbackBeforeCalls.add(key);
	}

	static int sumAndRecordIntegrator(int accumulator, String key, int value) {
		memberCallbackAfterCalls.add(Map.entry(key, value));
		return accumulator + value;
	}

	static final List<Map.Entry<String, Integer>> memberCallbackAfterCalls = new ArrayList<>();

	static void memberCallbackAfterInt(String key, int value) {
		memberCallbackAfterCalls.add(Map.entry(key, value));
	}

	static int sumIntegrator(int accumulator, String key, int value) {
		return accumulator + value;
	}

	@Test
	void objectAccumulator_typeChecks() {
		// keyHandler return type must be assignable TO integrator handler result param
		assertDoesNotThrow(() -> buildAccumulator(String.class, Object.class),
			"narrow keyHandler return should be assignable to wide integrator param");
		assertDoesNotThrow(() -> buildAccumulator(String.class, String.class),
			"equal types should be assignable");
		assertThrows(AssertionError.class, () -> buildAccumulator(Object.class, String.class),
			"wide keyHandler return should not be assignable to narrow integrator param");
	}

	private static ObjectAccumulator buildAccumulator(Class<?> keyHandlerReturn, Class<?> integratorHandlerParam) {
		DataType accType = DataType.known(Object.class);
		DataType keyType = DataType.known(String.class);
		DataType valueType = DataType.known(Integer.class);
		DataType khReturn = DataType.known(keyHandlerReturn);
		DataType integParam = DataType.known(integratorHandlerParam);

		return new ObjectAccumulator(
			TypedHandles.constant(accType, new Object()),
			new TypedHandle(
				MethodHandles.empty(MethodType.methodType(keyHandlerReturn, Object.class, String.class)),
				khReturn, List.of(accType, keyType)),
			new TypedHandle(
				MethodHandles.empty(MethodType.methodType(Object.class, Object.class, String.class, Integer.class, integratorHandlerParam)),
				accType, List.of(accType, keyType, valueType, integParam)),
			TypedHandles.identity(accType));
	}

	/**
	 * This is a lot of effort for code that never runs...
	 * but hopefully it pays off when we write the generator tests!
	 */
	static final class Unsummer {

		static ObjectEmitter emitter() throws NoSuchMethodException, IllegalAccessException {
			KnownType unsummer = DataType.known(Unsummer.class);
			KnownType member = DataType.known(Member.class);
			return new ObjectEmitter(
				new TypedHandle(
					MethodHandles.lookup().findConstructor(
						Unsummer.class,
						MethodType.methodType(void.class, int.class)
					),
					unsummer, List.of(INT)
				),
				new TypedHandle(
					MethodHandles.lookup().findVirtual(
						Unsummer.class,
						"hasNext",
						MethodType.methodType(boolean.class)
					),
					DataType.BOOLEAN, List.of(unsummer)
				),
				new TypedHandle(
					MethodHandles.lookup().findVirtual(
						Unsummer.class,
						"next",
						MethodType.methodType(Member.class)
					),
					member, List.of(unsummer)
				),
				new TypedHandle(
					MethodHandles.lookup().findVirtual(
						Member.class,
						"key",
						MethodType.methodType(String.class)
					),
					STRING, List.of(member)
				),
				new TypedHandle(
					MethodHandles.lookup().findVirtual(
						Member.class,
						"value",
						MethodType.methodType(int.class)
					),
					INT, List.of(member)
				)
			);
		}

		int numMembersEmitted = 0;
		int remainingValue;

		Unsummer(int targetValue) {
			this.remainingValue = targetValue;
		}

		boolean hasNext() {
			return remainingValue > 0;
		}

		record Member(String key, int value) {}

		Member next() {
			int value = Math.min(10 * (++numMembersEmitted), remainingValue);
			remainingValue -= value;
			return new Member("member" + numMembersEmitted, value);
		}

	}
}
