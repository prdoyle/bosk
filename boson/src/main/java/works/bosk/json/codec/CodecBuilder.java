package works.bosk.json.codec;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.json.codec.compiler.SpecCompiler;
import works.bosk.json.codec.interpreter.SpecInterpretingGenerator;
import works.bosk.json.codec.interpreter.SpecInterpretingParser;
import works.bosk.json.mapping.TypeMap;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.mapping.spec.SpecNode;

import static java.util.Objects.requireNonNull;

public class CodecBuilder {
	private final TypeMap typeMap;
	private final SpecCompiler compiler;
	private final Map<Package, Lookup> specialLookups = new HashMap<>();

	private CodecBuilder(TypeMap typeMap) {
		this.typeMap = typeMap;
		this.compiler = new SpecCompiler(typeMap, Map.copyOf(specialLookups));
	}

	/**
	 * @param typeMap is used to resolve any {@link works.bosk.json.mapping.spec.TypeRefNode}s
	 */
	public static CodecBuilder of(TypeMap typeMap) {
		return new CodecBuilder(typeMap);
	}

	/**
	 * When building codecs, uses the given {@link Lookup} object to find {@link MethodHandle}s
	 * for any class in the same package as the {@link Lookup}'s {@linkplain Lookup#lookupClass() lookup class}.
	 *
	 * @return {@code this}
	 */
	public CodecBuilder using(Lookup lookup) {
		specialLookups.put(requireNonNull(lookup.lookupClass().getPackage()), lookup);
		return this;
	}

	public CodecBuilder with(SpecNode... nodes) {
		return this;
	}

	public Codec buildInterpreter() {
		return new Codec() {
			@Override
			public Parser parserFor(JsonValueSpec spec) {
				return new SpecInterpretingParser(spec, typeMap);
			}

			@Override
			public Generator generatorFor(JsonValueSpec spec) {
				return new SpecInterpretingGenerator(spec, typeMap);
			}
		};
	}

	public Codec buildCompiled(JsonValueSpec... extraNodes) {
		return compiler.compile(extraNodes);
	}

	// TODO: We need to build one thing that can handle many types; Codec only handles one.
	public Codec build(JsonValueSpec... extraNodes) {
		return typeMap.settings().compiled()
			? buildCompiled(extraNodes)
			: buildInterpreter();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(CodecBuilder.class);
}
