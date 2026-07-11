package works.bosk.util;

import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import works.bosk.BoskContext.Tenant;
import works.bosk.Identifier;
import works.bosk.junit.InjectFrom;
import works.bosk.junit.InjectedTest;
import works.bosk.util.PerTenantValue.MultiTenant;
import works.bosk.util.PerTenantValue.NoTenant;

import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.BoskContext.Tenant.NONE;
import static works.bosk.BoskContext.Tenant.TenantId;
import static works.bosk.util.PerTenantValue.MultiTenant.multiTenant;

@InjectFrom(PerTenantValueTest.Scenario.Injector.class)
class PerTenantValueTest {
	static final TenantId t1 = new TenantId(Identifier.from("t1"));
	static final TenantId t2 = new TenantId(Identifier.from("t2"));
	static final TenantId t3 = new TenantId(Identifier.from("t3"));

	enum Scenario {
		NO_TENANT,
		MULTI_TENANT,
		;

		PerTenantValue<String> perTenant() {
			return switch (this) {
				case NO_TENANT -> NoTenant.just("sole");
				case MULTI_TENANT -> Stream.of(t1,t2,t3)
					.collect(multiTenant(t->t, t->t.tenant().toString()));
			};
		}

		Map<Tenant, String> map() {
			return switch (this) {
				case NO_TENANT -> Map.of(NONE, "sole");
				case MULTI_TENANT -> new TreeMap<>(Stream.of(t1,t2,t3).collect(toMap(
					t->t,
					t->t.tenant().toString()
				)));
			};
		}

		record Injector() implements works.bosk.junit.Injector {
			@Override
			public boolean supports(AnnotatedElement element, Class<?> elementType) {
				return elementType == Scenario.class;
			}

			@Override
			public List<?> values() {
				return List.of(Scenario.values());
			}
		}
	}

	record Seen(Tenant tenant, String value) {}

	@InjectedTest
	void forEach(Scenario scenario) {
		List<Seen> actual = new ArrayList<>();
		scenario.perTenant().forEach((tenant, value) -> actual.add(new Seen(tenant, value)));
		List<Seen> expected = new ArrayList<>();
		scenario.map().forEach((tenant, value) -> expected.add(new Seen(tenant, value)));
		assertEquals(expected, actual);
	}

	@InjectedTest
	void map(Scenario scenario) {
		PerTenantValue<String> mapped = scenario.perTenant().map(s -> s.toUpperCase(Locale.ROOT));
		List<Seen> actual = new ArrayList<>();
		mapped.forEach((tenant, value) -> actual.add(new Seen(tenant, value)));

		List<Seen> expected = new ArrayList<>();
		scenario.map().forEach((tenant, value) -> expected.add(new Seen(tenant, value.toUpperCase(Locale.ROOT))));
		assertEquals(expected, actual);
	}

	@Test
	void multi_with() {
		var mt_12 = Stream.of(t1, t2).collect(multiTenant(t->t, t->t.tenant().toString()));
		var mt_123 = mt_12
			.with(t2, "replaced")
			.with(t3, t3.tenant().toString());

		List<Seen> seen_12 = new ArrayList<>();
		mt_12.forEach((tenant, value) -> seen_12.add(new Seen(tenant, value)));
		assertEquals(List.of(new Seen(t1, "t1"), new Seen(t2, "t2")), seen_12,
			"Original is unchanged");

		List<Seen> seen_123 = new ArrayList<>();
		mt_123.forEach((tenant, value) -> seen_123.add(new Seen(tenant, value)));
		assertEquals(List.of(new Seen(t1, "t1"), new Seen(t2, "replaced"), new Seen(t3, "t3")), seen_123);
	}

	@Test
	void multi_without() {
		var mt_12 = Stream.of(t1, t2).collect(multiTenant(t->t, t->t.tenant().toString()));
		var mt_1 = mt_12
			.without(t2)
			.without(t3);

		List<Seen> seen_12 = new ArrayList<>();
		mt_12.forEach((tenant, value) -> seen_12.add(new Seen(tenant, value)));
		assertEquals(List.of(new Seen(t1, "t1"), new Seen(t2, "t2")), seen_12,
			"Original is unchanged");

		List<Seen> seen_1 = new ArrayList<>();
		mt_1.forEach((tenant, value) -> seen_1.add(new Seen(tenant, value)));
		assertEquals(List.of(new Seen(t1, "t1")), seen_1);
	}

	@InjectedTest
	void asNoTenant(Scenario scenario) {
		var original = switch (scenario) {
			case NO_TENANT -> NoTenant.just("value");
			case MULTI_TENANT -> MultiTenant.singleton(t1, "value");
		};
		assertEquals(NoTenant.just("value"), original.asNoTenant(t1));
	}

	@Test
	void wrongTenant() {
		assertThrows(IllegalArgumentException.class, () -> MultiTenant.singleton(t1, "value").asNoTenant(t2));
	}

}
