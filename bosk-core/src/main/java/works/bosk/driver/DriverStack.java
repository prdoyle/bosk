package works.bosk.driver;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

/**
 * Describes a sequence of layers of {@link BoskDriver} objects
 * to be used for processing bosk state updates.
 */
public class DriverStack {
	final List<DriverSpec> specs;

	private DriverStack(List<DriverSpec> specs) {
		this.specs = specs;
	}

	public static DriverStack of(DriverSpec... specs) {
		return new DriverStack(List.of(specs));
	}

	public DriverStack with(Mixin... mixins) {
		var newSpecs = this.specs;
		for (var m: mixins) {
			newSpecs = m.appliedTo(unmodifiableList(newSpecs));
		}
		return new DriverStack(List.copyOf(newSpecs));
	}

	public record DriverInstance(DriverSpec spec, BoskDriver driver){}

	/**
	 * @return a list of {@link DriverInstance} records describing all the drivers in the stack.
	 * The drivers are listed in the order they will process updates; that is,
	 * the same order they are listed in {@link #specs}.
	 * If there are <em>N</em> entries in {@link #specs}, the returned list will have <em>N+1</em>
	 * entries, because this method appends one additional entry representing the downstream driver.
	 */
	public List<DriverInstance> build(BoskInfo<?> boskInfo, BoskDriver downstream) {
		// This is all simpler using List.reversed(). We could rewrite it once we move to Java 21.
		Deque<DriverInstance> result = new ArrayDeque<>();
		result.add(new DriverInstance(
			new DriverSpec(emptyList(), DriverFactory.ofInstance(downstream)),
			downstream));
		BoskDriver currentDriver = downstream;
		for (var iter = specs.listIterator(specs.size()); iter.hasPrevious();) {
			var s = iter.previous();
			currentDriver = s.driverFactory().build(boskInfo, currentDriver);
			result.addFirst(new DriverInstance(s, currentDriver));
		}
		return List.copyOf(result);
	}
}
