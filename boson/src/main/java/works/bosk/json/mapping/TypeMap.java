package works.bosk.json.mapping;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.types.DataType;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class TypeMap {
	private final Settings settings;
	private final Map<DataType, JsonValueSpec> memo;
	private final AtomicBoolean isFrozen = new AtomicBoolean(false);

	private TypeMap(Settings settings, Map<DataType, JsonValueSpec> memo) {
		this.settings = settings;
		this.memo = memo;
	}

	public TypeMap(Settings settings) {
		this(settings, new LinkedHashMap<>());
	}

	public void freeze() {
		isFrozen.set(true);
	}

	public static TypeMap copyOf(TypeMap other) {
		return new TypeMap(other.settings, new LinkedHashMap<>(other.memo));
	}

	public static TypeMap empty(Settings settings) {
		TypeMap typeMap = new TypeMap(settings, Map.of());
		typeMap.freeze();
		return typeMap;
	}

	public Set<DataType> knownTypes() {
		return Set.copyOf(memo.keySet());
	}

	/**
	 * @throws IllegalArgumentException if there's no node for the given type
	 */
	public JsonValueSpec get(DataType type) {
		var result = memo.get(type);
		if (result == null) {
			throw new IllegalArgumentException("No spec for type " + type);
		}
		return result;
	}

	public JsonValueSpec computeIfAbsent(DataType type, Function<DataType, JsonValueSpec> func) {
		if (isFrozen.get()) {
			throw new IllegalStateException("TypeMap is frozen");
		}
		return requireNonNull(memo.computeIfAbsent(type, func));
	}

	public JsonValueSpec put(DataType type, JsonValueSpec newValue) {
		if (isFrozen.get()) {
			throw new IllegalStateException("TypeMap is frozen");
		}
		return memo.put(type, requireNonNull(newValue));
	}

	public record Settings(
		boolean compiled,
		boolean iterative,
		boolean optimize,
		boolean fewerSwitches
	) {
		public static Settings DEFAULT = new Settings(true, false, true, false);

		public Settings withFewerSwitches() {
			return new Settings(compiled, iterative, optimize, true);
		}
	}

	/**
	 * We hold settings here not because it makes sense, but because it's a convenient
	 * thing that everyone can access.
	 */
	public Settings settings() {
		return settings;
	}

	public String contentDescription() {
		return "{\n" + memo.entrySet().stream()
			.map(e -> "\"" + e.getKey() + "\": " + e.getValue())
			.collect(joining("\t\n"))
			+ "}";
	}
}
