package works.bosk;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import static org.openjdk.jmh.annotations.Mode.Throughput;

@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Throughput)
public class VirtualThreadBenchmark {

	@State(Scope.Benchmark)
	public static class BenchmarkState {
		final ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
		final ExecutorService realThreads = Executors.newThreadPerTaskExecutor(Thread::new);
		final ExecutorService pooledThreads = Executors.newFixedThreadPool(1);
		String result;

		@TearDown
		public void tearDown() {
			virtualThreads.close();
			realThreads.close();
			pooledThreads.close();
		}
	}

	@Benchmark
	public String baseline(BenchmarkState state) throws ExecutionException, InterruptedException {
		FutureTask<String> stringFutureTask = new FutureTask<>(() -> "hello");
		stringFutureTask.run();
		return stringFutureTask.get();
	}

	@Benchmark
	public Object virtualThreadPool(BenchmarkState state) throws ExecutionException, InterruptedException {
		return state.virtualThreads.submit(()->"hello").get();
	}

	@Benchmark
	public Object virtualThread(BenchmarkState state) throws InterruptedException {
		Thread.startVirtualThread(() -> state.result = "hello").join();
		return state.result;
	}

	@Benchmark
	public Object platformThreadPool(BenchmarkState state) throws ExecutionException, InterruptedException {
		return state.realThreads.submit(()->"hello").get();
	}

	@Benchmark
	public Object platformThread(BenchmarkState state) throws ExecutionException, InterruptedException {
		var thread = new Thread(() -> state.result = "hello");
		thread.start();
		thread.join();
		return state.result;
	}

	@Benchmark
	public Object singlePooledThread(BenchmarkState state) throws ExecutionException, InterruptedException {
		return state.pooledThreads.submit(()->"hello").get();
	}

}
