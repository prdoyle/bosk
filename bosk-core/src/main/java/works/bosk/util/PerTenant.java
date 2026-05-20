package works.bosk.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import org.pcollections.TreePMap;
import works.bosk.BoskContext.Tenant;
import works.bosk.BoskContext.Tenant.Established;
import works.bosk.BoskContext.Tenant.TenantId;

import static java.util.function.Function.identity;
import static works.bosk.BoskContext.Tenant.NONE;

/**
 * A data structure that needs separate versions in multitenant situations.
 */
public sealed interface PerTenant<T> {
	void forEach(BiConsumer<? super Established, ? super T> consumer);

	/**
	 * Not a multitenant situation: the {@link Tenant} is {@link Tenant#NONE NONE}.
	 */
	record SoleTenant<T>(T value) implements PerTenant<T> {
		@Override
		public void forEach(BiConsumer<? super Established, ? super T> consumer) {
			consumer.accept(NONE, value);
		}

		public static <TT> SoleTenant<TT> just(TT value) {
			return new SoleTenant<>(value);
		}
	}

	/**
	 * Multitenant situation: there are zero or more tenants, each with its
	 * own version of the data.
	 * <p>
	 * Tenants are ordered by their ID.
	 */
	record MultiTenant<T>(SortedMap<TenantId, T> values) implements PerTenant<T> {
		public MultiTenant {
			// The values should always be a TreePMap so that we can do efficient
			// nondestructive updates, but we don't want to force all users of this
			// library to depend explicitly on the pcollections library.
			if (!(values instanceof TreePMap<TenantId,T>)) {
				values = TreePMap.from(values);
			}
		}

		@Override
		public void forEach(BiConsumer<? super Established, ? super T> consumer) {
			values.forEach(consumer);
		}

		public MultiTenant<T> with(TenantId key, T value) {
			return new MultiTenant<>(treePMap().plus(key, value));
		}

		public MultiTenant<T> withAll(Map<TenantId, T> additionalValues) {
			return new MultiTenant<>(treePMap().plusAll(additionalValues));
		}

		public MultiTenant<T> without(TenantId key) {
			return new MultiTenant<>(treePMap().minus(key));
		}

		public static <IN, OUT> Collector<Entry<TenantId, IN>, ?, MultiTenant<OUT>> withValues(Function<IN, OUT> valueMapper) {
			class Accumulator {
				TreePMap<TenantId, OUT> map = org.pcollections.TreePMap.empty();
				void accumulate(Entry<TenantId, IN> e) { map = map.plus(e.getKey(), valueMapper.apply(e.getValue())); }
				Accumulator combine(Accumulator other) { map = map.plusAll(other.map); return this; }
				MultiTenant<OUT> finish() { return new MultiTenant<>(map); }
			}
			return Collector.of(
				Accumulator::new,
				Accumulator::accumulate,
				Accumulator::combine,
				Accumulator::finish
			);
		}

		public static <TT> Collector<Entry<TenantId, TT>, ?, MultiTenant<TT>> collector() {
			return withValues(identity());
		}

		private TreePMap<TenantId, T> treePMap() {
			return (TreePMap<TenantId, T>) values;
		}
	}

}
