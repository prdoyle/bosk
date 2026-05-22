package works.bosk.junit;

import java.lang.reflect.AnnotatedElement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import works.bosk.junit.InjectFromHappyPathTests.BaseValue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QualifiedInjectionTests {

	// Simple injector producing two string values
	record StringInjector() implements Injector {
		@Override
		public boolean supports(AnnotatedElement element, Class<?> elementType) {
			return elementType == String.class;
		}

		@Override
		public List<String> values() {
			return List.of("A", "B");
		}
	}

	@Nested
	@InjectFrom({StringInjector.class})
	class ParameterParameterTest {
		static Set<String> differentQualifierObservations = new HashSet<>();
		static Set<String> sameQualifierObservations = new HashSet<>();

		@InjectedTest
		void sameQualifier(@Injected("x") String a, @Injected("x") String b) {
			sameQualifierObservations.add(a + ":" + b);
		}

		@InjectedTest
		void differentQualifier(@Injected("x") String a, @Injected("y") String b) {
			differentQualifierObservations.add(a + ":" + b);
		}

		@AfterAll
		static void check() {
			assertEquals(Set.of("A:A", "B:B"), sameQualifierObservations);
			assertEquals(Set.of("A:A", "A:B", "B:A", "B:B"), differentQualifierObservations);
		}
	}

	@Nested
	@InjectFrom({BaseValue.class})
	class EnumQualifierParameterTest {
		static Set<String> differentQualifierObservations = new HashSet<>();
		static Set<String> sameQualifierObservations = new HashSet<>();

		@InjectedTest
		void sameQualifier(@Injected("x") BaseValue a, @Injected("x") BaseValue b) {
			sameQualifierObservations.add(a + ":" + b);
		}

		@InjectedTest
		void differentQualifier(@Injected("x") BaseValue a, @Injected("y") BaseValue b) {
			differentQualifierObservations.add(a + ":" + b);
		}

		@AfterAll
		static void check() {
			assertEquals(Set.of("FIRST:FIRST", "SECOND:SECOND"), sameQualifierObservations);
			assertEquals(Set.of("FIRST:FIRST", "FIRST:SECOND", "SECOND:FIRST", "SECOND:SECOND"), differentQualifierObservations);
		}
	}

	@Nested
	@InjectFields
	@InjectFrom({StringInjector.class})
	class FieldFieldTest {
		static Set<String> observations = new HashSet<>();

		@Injected("x") String f0;
		@Injected("y") String f1;
		@Injected("y") String f2;

		@Test
		void test() {
			observations.add(f0 + ":" + f1 + ":" + f2);
		}

		@AfterAll
		static void checkDefaults() {
			// f0 independent, f1 and f2 linked -> cartesian product where second and third are equal
			assertEquals(Set.of("A:A:A", "A:B:B", "B:A:A", "B:B:B"), observations);
		}

	}

	@Nested
	@InjectFields
	@InjectFrom({StringInjector.class})
	class FieldParameterTest {
		static Set<String> differentQualifierObservations = new HashSet<>();
		static Set<String> sameQualifierObservations = new HashSet<>();

		@Injected("x") String fieldX;

		@InjectedTest
		void sameQualifier(@Injected("x") String paramX) {
			sameQualifierObservations.add(fieldX + ":" + paramX);
		}

		@InjectedTest
		void differentQualifier(@Injected("y") String paramY) {
			differentQualifierObservations.add(fieldX + ":" + paramY);
		}

		@AfterAll
		static void check() {
			assertEquals(Set.of("A:A", "B:B"), sameQualifierObservations);
			assertEquals(Set.of("A:A", "A:B", "B:A", "B:B"), differentQualifierObservations);
		}
	}
}
