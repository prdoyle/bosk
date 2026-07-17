package works.bosk.util;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jspecify.annotations.NonNull;
import works.bosk.BoskConfig.TenancyModel;
import works.bosk.BoskConfig.TenancyModel.Implicit;
import works.bosk.BoskContext;
import works.bosk.BoskContext.Tenant;
import works.bosk.BoskContext.Tenant.Established;
import works.bosk.BoskContext.Tenant.NotEstablished;
import works.bosk.util.PerTenantValue.MultiTenant;

/**
 * A variable that is specific to an {@link Tenant.Established established tenant}.
 * <p>
 * When a tenant is established in the {@link BoskContext}, only that tenant's value is accessible.
 * Attempts to access another tenant's value will throw an {@link IllegalStateException}.
 * If the tenant is {@link NotEstablished not established}, then all tenant values are accessible;
 * in other words, establishing a tenant means dropping privileges.
 * <p>
 * A result of this permission system is that the access checks are a bit complex.
 * It would be simple if we simply required no established tenant for the {@code For} methods,
 * but in the {@link works.bosk.BoskConfig.TenancyModel.Implicit implicit} tenancy models,
 * the tenant is established automatically, and the user has no way to shed that and regain privilege.
 * <p>
 * Methods that access a specific tenant's value typically have {@code For} in the name,
 * except for {@link #replaceWith(PerTenantValue) replaceWith}.
 * Other methods like {@link #get()} access the value for the currently established tenant.
 * <p>
 * This is inspired by {@link ThreadLocal}, but don't take the analogy too far:
 * this class is not thread-local so users must take into account concurrent access;
 * for example, consecutive calls to {@link #get()} can return different values.
 * The methods are atomic, though.
 *
 * @param <T>
 */
public final class TenantLocal<T> {
	final BoskContext context;
	final AtomicReference<ConcurrentHashMap<Established, T>> values = new AtomicReference<>(new ConcurrentHashMap<>());

	private TenantLocal(BoskContext context) {
		this.context = context;
	}

	public static <T> TenantLocal<T> in(BoskContext context) {
		return new TenantLocal<>(context);
	}

	public T get() {
		return values.get().get(context.getEstablishedTenant());
	}

	public T set(T newValue) {
		return values.get().put(context.getEstablishedTenant(), newValue);
	}

	public boolean replace(T expected, T newValue) {
		return values.get().replace(context.getEstablishedTenant(), expected, newValue);
	}

	public T computeIfAbsent(Function<Tenant.Established, T> mappingFunction) {
		return values.get().computeIfAbsent(context.getEstablishedTenant(), mappingFunction);
	}

	public T remove() {
		return values.get().remove(context.getEstablishedTenant());
	}

	/**
	 * Affects all tenants, and so the permission model is a little different from the {@code For} methods:
	 * the permitted access depends on the {@link works.bosk.BoskConfig.TenancyModel tenancy model}.
	 * <p>
	 * Note that, for {@link TenancyModel.Fixed},
	 * {@code newValues} must be a singleton {@link PerTenantValue.MultiTenant MultiTenant} value
	 * for the tenant that is automatically established by that model.
	 * {@link PerTenantValue.NoTenant NoTenant} is not allowed.
	 */
	public void replaceWith(PerTenantValue<T> newValues) {
		checkReplacementAccess(newValues);
		var newMap = new ConcurrentHashMap<Established, T>();
		newValues.forEach(newMap::put);
		values.set(newMap);
	}

	public T getFor(Tenant.Established tenant) {
		checkTenantAccess(tenant);
		return values.get().get(tenant);
	}

	public T setFor(Tenant.Established tenant, T newValue) {
		checkTenantAccess(tenant);
		return values.get().put(tenant, newValue);
	}

	public T computeIfAbsentFor(Established tenant, Function<? super Established, ? extends T> mappingFunction) {
		checkTenantAccess(tenant);
		return values.get().computeIfAbsent(tenant, mappingFunction);
	}

	public void forEach(BiConsumer<? super Established, ? super T> action) {
		checkAllAccess();
		values.get().forEach(action);
	}

	@Override
	public String toString() {
		return "TenantLocal{" + values + "}";
	}

	/**
	 * The "For" methods are called from "management" code that is aware of tenants and needs to
	 * change a particular tenant's state. We allow this only if there is no established tenant
	 * (ie. we haven't dropped privileges) or if the established tenant is the one we're trying to access.
	 * Either way, we're good.
	 *
	 * @param requestedTenant is null if there's no specific tenant whose state we're trying to access
	 */
	private void checkTenantAccess(Tenant.@NonNull Established requestedTenant) {
		switch (context.getTenant()) {
			case NotEstablished _ -> { /* all is well */ }
			case Established t when t.equals(requestedTenant) -> { /* all is well */ }
			case Established _ ->
				throw new IllegalStateException("Cannot access " + requestedTenant + " when tenant is " + context.getTenant());
		}
	}

	private void checkAllAccess() {
		if (context.tenancyModel() instanceof Implicit) {
			return; // there are no forbidden tenants in implicit models
		}
		final Tenant contextTenant = context.getTenant();
		if (!(contextTenant instanceof NotEstablished)) {
			throw new IllegalStateException("Cannot access all tenants when tenant is " + contextTenant);
		}
	}

	/**
	 * Replacement generally affects all tenants, so it's only allowed if
	 * (1) there's no established tenant in the {@link BoskContext},
	 * or (2) the tenancy model is {@link Implicit} and {@code newValues} are for the tenant that is automatically established by that model.
	 * (The old values are assumed to be for the right tenant, since they were validated when they were set,
	 * and the tenancy model can't change.)
	 */
	private void checkReplacementAccess(PerTenantValue<T> newValues) {
		final Tenant contextTenant = context.getTenant();
		switch (context.tenancyModel()) {
			case TenancyModel _
				when contextTenant instanceof NotEstablished _ -> { /* always ok */ }
			case TenancyModel.None _
				when newValues instanceof PerTenantValue.NoTenant<T> -> { /* ok */ }
			case TenancyModel.Fixed(var id)
				when newValues instanceof MultiTenant<T>(var map)
				&& map.keySet().equals(Set.of(Tenant.setTo(id))) -> { /* ok */ }
			default ->
				throw new IllegalStateException("Cannot replace value when tenant is " + contextTenant);
		}
	}

}
