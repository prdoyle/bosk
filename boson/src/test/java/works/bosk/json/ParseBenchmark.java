package works.bosk.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import works.bosk.json.TestUtils.JustScalars;
import works.bosk.json.TestUtils.Month;
import works.bosk.json.TestUtils.OneOfEach;
import works.bosk.json.codec.CharArrayReader;
import works.bosk.json.codec.CodecBuilder;
import works.bosk.json.codec.Parser;
import works.bosk.json.mapping.TypeMap;
import works.bosk.json.mapping.TypeScanner;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.types.DataType;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static works.bosk.json.TestUtils.JUST_SCALARS;
import static works.bosk.json.TestUtils.ONE_OF_EACH;
import static works.bosk.json.mapping.TypeMap.Settings.DEFAULT;

@BenchmarkMode(Throughput)
@State(Scope.Thread)
@Fork(3)
@Warmup(iterations = 8, time = 1)
@Measurement(iterations = 3, time = 1, timeUnit = SECONDS)
public class ParseBenchmark {
	private char[] json;
	private ObjectReader objectReader;
	private ManualTest manualTest;
	private Parser interpreter;
	private Parser interpreterExperimental;
	private Parser compiled;
	private Parser compiledExperimental;

	@Setup(Level.Iteration) // Called once per iteration
	public void setup() {
		Class<?> targetClass;
		if (true) {
			targetClass = OneOfEach.class;
			json = ONE_OF_EACH.toCharArray();
		} else {
			targetClass = JustScalars.class;
			json = JUST_SCALARS;

		}
		var objectMapper = new ObjectMapper();
		objectReader = objectMapper.readerFor(targetClass);
		manualTest = new ManualTest();

		DataType targetType = DataType.of(targetClass);

		TypeScanner defaultTS = new TypeScanner(DEFAULT);
		defaultTS.specify(DataType.of(Month.class), Month.specNode());
		defaultTS.scan(targetType);
		TypeMap defaultTypeMap = defaultTS.build();
		JsonValueSpec defaultSpec = defaultTypeMap.get(targetType);
		interpreter = CodecBuilder.of(defaultTypeMap)
			.buildInterpreter().parserFor(defaultSpec);
		compiled = CodecBuilder.of(defaultTypeMap)
			.buildCompiled().parserFor(defaultSpec);

		TypeScanner experimentalTS = new TypeScanner(DEFAULT.withFewerSwitches());
		experimentalTS.specify(DataType.of(Month.class), Month.specNode());
		experimentalTS.scan(targetType);
		var experimentalTypeMap = experimentalTS.build();
		JsonValueSpec experimentalSpec = experimentalTypeMap.get(targetType);
		interpreterExperimental = CodecBuilder.of(experimentalTypeMap)
			.buildInterpreter().parserFor(experimentalSpec);
		compiledExperimental = CodecBuilder.of(experimentalTypeMap)
			.buildCompiled().parserFor(experimentalSpec);
	}

	@Benchmark
	public Object jackson() throws IOException {
		return objectReader.readValue(new java.io.CharArrayReader(json));
	}

	@Benchmark
	public Object manual() throws IOException {
		manualTest.init();
		return manualTest.parse();
	}

//	@Benchmark
	public Object interpreter_default() throws IOException {
		return interpreter.parse(new CharArrayReader(json, 0));
	}

//	@Benchmark
	public Object interpreter_experimental() throws IOException {
		return interpreterExperimental.parse(new CharArrayReader(json, 0));
	}

//	@Benchmark
	public Object compiled_default() throws IOException {
		return compiled.parse(new CharArrayReader(json, 0));
	}

	@Benchmark
	public Object compiled_experimental() throws IOException {
		return compiledExperimental.parse(new CharArrayReader(json, 0));
	}
}
