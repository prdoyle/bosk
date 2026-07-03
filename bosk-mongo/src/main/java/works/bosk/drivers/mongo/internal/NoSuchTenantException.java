package works.bosk.drivers.mongo.internal;

import works.bosk.BoskContext.Tenant.TenantId;

/**
 * Thrown internally when a root document for the current tenant
 * is not found in MongoDB, meaning the tenant does not exist.
 */
final class NoSuchTenantException extends RuntimeException {
	final TenantId tenant;

	NoSuchTenantException(TenantId tenant, String message) {
		super(message);
		this.tenant = tenant;
	}
}
