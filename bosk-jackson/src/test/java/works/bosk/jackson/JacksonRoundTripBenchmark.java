package works.bosk.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.atomic.AtomicReference;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.AbstractBoskTest;
import works.bosk.AbstractRoundTripTest;
import works.bosk.Bosk;
import works.bosk.BoskDriver;
import works.bosk.DriverStack;
import works.bosk.Identifier;
import works.bosk.Path;
import works.bosk.Reference;
import works.bosk.exceptions.InvalidTypeException;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static works.bosk.jackson.JacksonPluginConfiguration.defaultConfiguration;

@Fork(0)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@OutputTimeUnit(MICROSECONDS)
public class JacksonRoundTripBenchmark extends AbstractRoundTripTest {

	@State(Scope.Benchmark)
	public static class BenchmarkState {
		private Bosk<TestRoot> bosk;
		private JacksonPlugin jacksonPlugin;
		private ObjectMapper mapper;
		private BoskDriver driver;
		private BoskDriver downstreamDriver;
		private Reference<TestRoot> rootRef;
		private TestRoot root1, root2;

		final Identifier parentID = Identifier.from("parent");
		final Identifier child1ID = Identifier.from("child1");

		@Setup(Level.Trial)
		public void setup() throws InvalidTypeException, JsonProcessingException {
			AtomicReference<BoskDriver> downstreamRef = new AtomicReference<>();
			this.bosk = setUpBosk(DriverStack.of(
				jacksonRoundTripFactory(defaultConfiguration()),
				(b,d) -> {
					downstreamRef.set(d);
					return d;
				}
			));
			this.driver = bosk.driver();
			this.downstreamDriver = downstreamRef.get();
			this.jacksonPlugin = new JacksonPlugin();
			this.mapper = new ObjectMapper().registerModule(jacksonPlugin.moduleFor(bosk));
			rootRef = bosk.rootReference();
			TestRoot localRoot = root1 = rootRef.value();

			// Make a separate identical state object, cloning via JSON
			String json = mapper.writerFor(rootRef.targetClass()).writeValueAsString(root1);
			root2 = mapper.readerFor(rootRef.targetClass()).readValue(json);
		}

	}

	@Benchmark
	@BenchmarkMode(AverageTime)
	public void replacementOverhead(BenchmarkState state) {
		state.downstreamDriver.submitReplacement(state.rootRef, state.root2);
		state.downstreamDriver.submitReplacement(state.rootRef, state.root1);
	}

	@Benchmark
	@BenchmarkMode(AverageTime)
	public void replacement(BenchmarkState state) {
		state.driver.submitReplacement(state.rootRef, state.root2);
		state.driver.submitReplacement(state.rootRef, state.root1);
	}

}
