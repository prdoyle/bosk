package works.bosk.junit;

import java.util.Comparator;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.MethodOrdererContext;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AnteTestExtension implements BeforeEachCallback, AfterEachCallback {

	private static final String ANTE_FAILED = "anteTestFailed";

	@Override
	public void afterEach(ExtensionContext context) {
		if (context.getRequiredTestMethod().isAnnotationPresent(Ante.class)) {
			context.getExecutionException().ifPresent(_ ->
				classStore(context).put(ANTE_FAILED, true)
			);
		}
	}

	@Override
	public void beforeEach(ExtensionContext context) {
		boolean anteFailed = classStore(context)
			.getOrDefault(ANTE_FAILED, Boolean.class, false);
		assumeTrue(!anteFailed, "Ante test failed — skipping remaining tests");
	}

	private static Store classStore(ExtensionContext context) {
		// Look for the lowest level that doesn't have a test method: that would be the class-level context
		ExtensionContext current = context;
		while (current.getTestMethod().isPresent()) {
			current = current.getParent()
				.orElseThrow(() -> new IllegalStateException("No class context found"));
		}
		return current.getStore(Namespace.create(AnteTestExtension.class));
	}

	public static class Orderer implements MethodOrderer {
		@Override
		public void orderMethods(MethodOrdererContext context) {
			context.getMethodDescriptors().sort(
				Comparator.comparingInt(m -> m.isAnnotated(Ante.class) ? -1 : 1));
		}
	}
}
