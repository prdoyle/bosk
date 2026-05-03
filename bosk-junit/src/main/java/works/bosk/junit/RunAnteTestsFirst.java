package works.bosk.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Runs {@link Ante} tests before other tests,
 * and if one of them fails, skips all remaining tests.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(AnteTestExtension.class)
@TestMethodOrder(AnteTestExtension.Orderer.class)
public @interface RunAnteTestsFirst {}
