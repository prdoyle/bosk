package works.bosk.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import works.bosk.BoskConfig.TenancyModel;
import works.bosk.BoskContext;
import works.bosk.BoskContext.Context;
import works.bosk.BoskContext.Tenant;
import works.bosk.BoskContext.Tenant.TenantId;
import works.bosk.BoskContextTest;
import works.bosk.Identifier;
import works.bosk.MapValue;
import works.bosk.junit.Ante;
import works.bosk.junit.RunAnteTestsFirst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunAnteTestsFirst
class TenantLocalTest {
	BoskContext context;
	TenantLocal<String> tenantLocal;

	final TenantId tenant1 = Tenant.setTo(Identifier.from("tenant1"));
	final TenantId tenant2 = Tenant.setTo(Identifier.from("tenant2"));

	@BeforeEach
	void setup() {
		context = BoskContextTest.newContext(()->new Context(
			Tenant.NOT_ESTABLISHED,
			MapValue.empty()), "testBoskName", TenancyModel.EXPLICIT
		);
		tenantLocal = TenantLocal.in(context);
	}

	@Ante
	@Test
	void basicFunctionality() {
		assertThrows(IllegalStateException.class, () -> tenantLocal.set("test"),
			"Cannot set value when tenant is not established");

		try (var _ = context.withTenant(tenant1)) {
			tenantLocal.set("value1");
			assertEquals("value1", tenantLocal.get());
		}
		try (var _ = context.withTenant(tenant2)) {
			tenantLocal.set("value2");
			assertEquals("value2", tenantLocal.get());
		}
		try (var _ = context.withTenant(tenant1)) {
			assertEquals("value1", tenantLocal.get());
		}
	}

	@Test
	void testAllTheThings() {
		try (var _ = context.withTenant(tenant1)) {
			assertNull(tenantLocal.get());
			tenantLocal.set("value1");
			assertEquals("value1", tenantLocal.get());
			tenantLocal.replace("value1", "value2");
			assertEquals("value2", tenantLocal.get());
			tenantLocal.replace("value1", "value1");
			assertEquals("value2", tenantLocal.get(),
				"replace does nothing when expected value doesn't match");
			tenantLocal.remove();
			assertNull(tenantLocal.get());
			tenantLocal.computeIfAbsent(_ -> "value1");
			assertEquals("value1", tenantLocal.get());
			tenantLocal.computeIfAbsent(_ -> "value2");
			assertEquals("value1", tenantLocal.get(),
				"computeIfAbsent does nothing when value is already present");
		}
	}

}
