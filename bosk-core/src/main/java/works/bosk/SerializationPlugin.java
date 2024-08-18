package works.bosk;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.pcollections.ConsPStack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.annotations.DeserializationPath;
import works.bosk.annotations.Enclosing;
import works.bosk.annotations.Polyfill;
import works.bosk.annotations.Self;
import works.bosk.annotations.VariantCaseMap;
import works.bosk.exceptions.DeserializationException;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.exceptions.MalformedPathException;
import works.bosk.exceptions.ParameterUnboundException;
import works.bosk.exceptions.UnexpectedPathException;

import static java.lang.reflect.Modifier.isPrivate;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Objects.requireNonNull;

/**
 * A "Plugin", for now, is a thing that translates Bosk objects for interfacing
 * with the outside world.  One fine day, we'll think of a better name.
 *
 * <p>
 * Serialization systems are generally not good at allowing custom logic to
 * supply any context. This class works around that limitation by supplying a
 * place to put some context, maintained using {@link ThreadLocal}s, and managed
 * using the {@link DeserializationScope} auto-closeable to make sure the thread-local context
 * state is managed correctly.
 *
 * <p>
 * Generally, applications create one instance of each plugin they need.
 * Instances are thread-safe. The only case where you might want another
 * instance is if you need to perform a second, unrelated, nested
 * deserialization while one is already in progress on the same thread. It's
 * hard to think of a reason that an application would want to do this.
 *
 * @author pdoyle
 *
 */
public abstract class SerializationPlugin {
	private final ThreadLocal<DeserializationScope> currentScope = ThreadLocal.withInitial(this::outermostScope);

	public final DeserializationScope newDeserializationScope(Path newPath) {
		DeserializationScope outerScope = currentScope.get();
		DeserializationScope newScope = new NestedDeserializationScope(
			outerScope,
			newPath,
			outerScope.bindingEnvironment());
		currentScope.set(newScope);
		return newScope;
	}

	public final DeserializationScope newDeserializationScope(Reference<?> ref) {
		return newDeserializationScope(ref.path());
	}

	public final DeserializationScope overlayScope(BindingEnvironment env) {
		DeserializationScope outerScope = currentScope.get();
		DeserializationScope newScope = new NestedDeserializationScope(
			outerScope,
			outerScope.path(),
			outerScope.bindingEnvironment().overlay(env));
		currentScope.set(newScope);
		return newScope;
	}

	public final DeserializationScope entryDeserializationScope(Identifier entryID) {
		DeserializationScope outerScope = currentScope.get();
		DeserializationScope newScope = new NestedDeserializationScope(
			outerScope,
			outerScope.path().then(entryID.toString()),
			outerScope.bindingEnvironment());
		currentScope.set(newScope);
		return newScope;
	}

	public final DeserializationScope nodeFieldDeserializationScope(Class<?> nodeClass, String fieldName) {
		DeserializationPath annotation = infoFor(nodeClass).annotatedParameters_DeserializationPath.get(fieldName);
		DeserializationScope outerScope = currentScope.get();
		if (annotation == null) {
			DeserializationScope newScope = new NestedDeserializationScope(
				outerScope,
				outerScope.path().then(fieldName),
				outerScope.bindingEnvironment());
			currentScope.set(newScope);
			return newScope;
		} else {
			try {
				Path path = Path
					.parseParameterized(annotation.value())
					.boundBy(outerScope.bindingEnvironment());
				if (path.numParameters() == 0) {
					DeserializationScope newScope = new NestedDeserializationScope(
						outerScope,
						path,
						outerScope.bindingEnvironment());
					currentScope.set(newScope);
					return newScope;
				} else {
					throw new ParameterUnboundException(
						"Unbound parameters in @"
							+ DeserializationPath.class.getSimpleName() + "(\"" + path + "\") "
							+ nodeClass.getSimpleName() + "." + fieldName + " ");
				}
			} catch (MalformedPathException e) {
				throw new MalformedPathException("Invalid DeserializationPath for "
					+ nodeClass.getSimpleName()
					+ "." + fieldName
					+ ": " + e.getMessage(), e);
			}
		}
	}

	private DeserializationScope outermostScope() {
		return new OutermostDeserializationScope();
	}

	public static abstract class DeserializationScope implements AutoCloseable {
		DeserializationScope(){}

		public abstract Path path();
		public abstract BindingEnvironment bindingEnvironment();

		@Override public abstract void close();
	}

	private static final class OutermostDeserializationScope extends DeserializationScope {
		@Override public Path path() { return Path.empty(); }
		@Override public BindingEnvironment bindingEnvironment() { return BindingEnvironment.empty(); }

		@Override
		public void close() {
			throw new IllegalStateException("Outermost scope should never be closed");
		}
	}

	@Value
	@EqualsAndHashCode(callSuper = false)
	private class NestedDeserializationScope extends DeserializationScope {
		DeserializationScope outer;
		Path path;
		BindingEnvironment bindingEnvironment;

		@Override
		public void close() {
			currentScope.set(requireNonNull(outer));
		}
	}

	/**
	 * Turns <code>parameterValuesByName</code> into a list suitable for
	 * passing to a constructor, in the order indicated by
	 * <code>parametersByName</code>.
	 *
	 *
	 * @param parameterValuesByName values read from the input. <em>Modified by this method.</em>
	 * @param parametersByName ordered map of constructor {@link Parameter}s.
	 * @return {@link List} of parameter values to pass to the constructor, in
	 * the same order as in <code>parametersByName</code>. Missing values are
	 * supplied where possible, such as <code>Optional.empty()</code> and
	 * {@link Enclosing} references.
	 */
	public final List<Object> parameterValueList(Class<?> nodeClass, Map<String, Object> parameterValuesByName, LinkedHashMap<String, Parameter> parametersByName, BoskInfo<?> boskInfo) {
		List<Object> parameterValues = new ArrayList<>();
		for (Entry<String, Parameter> entry: parametersByName.entrySet()) {
			String name = entry.getKey();
			Parameter parameter = entry.getValue();
			Class<?> type = parameter.getType();
			Reference<?> implicitReference = findImplicitReferenceIfAny(nodeClass, parameter, boskInfo);

			Object value = parameterValuesByName.remove(name);
			if (value == null) {
				// Field is absent in the input
				if (implicitReference != null) {
					parameterValues.add(implicitReference);
				} else if (Optional.class.equals(type)) {
					parameterValues.add(Optional.empty());
				} else if (Phantom.class.equals(type)) {
					parameterValues.add(Phantom.empty());
				} else {
					Object polyfillIfAny = infoFor(nodeClass).polyfills().get(name);
					Path path = currentScope.get().path();
					if ("id".equals(name) && !path.isEmpty()) {
						// If the object is an entry in a Catalog or a key in a SideTable, we can determine its ID
						Reference<Object> enclosingRef;
						try {
							enclosingRef = boskInfo.rootReference().then(Object.class, path.truncatedBy(1));
						} catch (InvalidTypeException e) {
							throw new AssertionError("Non-empty path must have an enclosing reference: " + path, e);
						}
						if (AddressableByIdentifier.class.isAssignableFrom(enclosingRef.targetClass())) {
							parameterValues.add(Identifier.from(path.lastSegment()));
						} else {
							throw new DeserializationException("Missing id field for object at " + path);
						}
					} else if (polyfillIfAny != null) {
						LOGGER.info("{} used polyfill value for {}.{}", getClass().getSimpleName(), nodeClass.getSimpleName(), name);
						parameterValues.add(polyfillIfAny);
					} else {
						throw new DeserializationException("Missing field \"" + name + "\" at " + path);
					}
				}
			} else if (implicitReference == null) {
				parameterValues.add(value);
			} else {
				throw new DeserializationException("Unexpected field \"" + name + "\" for implicit reference");
			}
		}
		if (!parameterValuesByName.isEmpty()) {
			throw new DeserializationException("Unrecognized fields: " + parameterValuesByName.keySet());
		}
		return parameterValues;
	}

	public static boolean isSelfReference(Class<?> nodeClass, Parameter parameter) {
		return infoFor(nodeClass).annotatedParameters_Self().contains(parameter.getName());
	}

	public static boolean isEnclosingReference(Class<?> nodeClass, Parameter parameter) {
		return infoFor(nodeClass).annotatedParameters_Enclosing().contains(parameter.getName());
	}

	public static boolean hasDeserializationPath(Class<?> nodeClass, Parameter parameter) {
		return infoFor(nodeClass).annotatedParameters_DeserializationPath().containsKey(parameter.getName());
	}

	/**
	 * @throws InvalidTypeException if the given class has no unique variant case map
	 */
	@NotNull
	public static MapValue<Type> getVariantCaseMap(Class<?> nodeClass) throws InvalidTypeException {
		var result = getVariantCaseMapIfAny(nodeClass);
		if (result == null) {
			throw new InvalidTypeException(nodeClass + " has no variant case map");
		} else {
			return result;
		}
	}

	/**
	 * @return null if the class has no variant case map
	 * @throws InvalidTypeException if the given class has no unique variant case map
	 */
	@Nullable
	public static MapValue<Type> getVariantCaseMapIfAny(Class<?> nodeClass) throws InvalidTypeException {
		if (VariantNode.class.isAssignableFrom(nodeClass)) {
			return infoFor(nodeClass).variantCaseMap().ifAny();
		} else {
			// We don't want to even call infoFor on types that aren't StateTreeNodes
			return null;
		}
	}

	public <R extends StateTreeNode> void initializeEnclosingPolyfills(Reference<?> target, BoskDriver<R> driver) {
		if (!ANY_POLYFILLS.get()) {
			return;
		}
		/*
		Evolution note: we should be able to make this more efficient.
		For the bosk state tree, we recursively analyze all the node types upfront.
		When that process is finished, a dataflow analysis over the ParameterInfo graph
		could determine, for any given type, whether that type ever occurs inside a polyfill.
		The common case is likely "no", and we could quickly dispense with all polyfill concerns
		for all references to that type. In particular, when there are no polyfills at all,
		we could quickly determine that there's nothing to do. In the absence of recursive
		datatypes, a reverse postorder walk over the ParameterInfo objects should converge in a single pass.
		 */
		if (!target.path().isEmpty()) {
			try {
				initializePolyfills(target.enclosingReference(Object.class), driver);
			} catch (InvalidTypeException e) {
				throw new AssertionError("Every non-root reference has an enclosing reference: " + target);
			}
		}
	}

	private <R extends StateTreeNode, T> void initializePolyfills(Reference<T> ref, BoskDriver<R> driver) {
		initializeEnclosingPolyfills(ref, driver);
		if (!ref.path().isEmpty()) {
			Class<?> enclosing;
			try {
				enclosing = ref.enclosingReference(Object.class).targetClass();
			} catch (InvalidTypeException e) {
				throw new AssertionError("Every non-root reference must have an enclosing reference: " + ref);
			}
			if (enclosing.isRecord() || StateTreeNode.class.isAssignableFrom(enclosing)) {
				Object result = infoFor(enclosing).polyfills().get(ref.path().lastSegment());
				if (result != null) {
					driver.submitInitialization(ref, ref.targetClass().cast(result));
				}
			}
		}
	}

	private Reference<?> findImplicitReferenceIfAny(Class<?> nodeClass, Parameter parameter, BoskInfo<?> boskInfo) {
		if (isSelfReference(nodeClass, parameter)) {
			Class<?> targetClass = ReferenceUtils.rawClass(ReferenceUtils.parameterType(parameter.getParameterizedType(), Reference.class, 0));
			return selfReference(targetClass, boskInfo);
		} else if (isEnclosingReference(nodeClass, parameter)) {
			Class<?> targetClass = ReferenceUtils.rawClass(ReferenceUtils.parameterType(parameter.getParameterizedType(), Reference.class, 0));
			Reference<Object> selfRef = selfReference(Object.class, boskInfo);
			try {
				return selfRef.enclosingReference(targetClass);
			} catch (InvalidTypeException e) {
				// TODO: Validation needs to check that every location
				// where this type appears in the document tree is
				// contained in a document of the target class.
				throw new UnexpectedPathException("Enclosing reference validation: Error looking up Enclosing ref \"" + parameter.getName() + "\": " + e.getMessage(), e);
			}
		} else {
			return null;
		}
	}

	private <T> Reference<T> selfReference(Class<T> targetClass, BoskInfo<?> boskInfo) throws AssertionError {
		Path currentPath = currentScope.get().path();
		try {
			return boskInfo.rootReference().then(targetClass, currentPath);
		} catch (InvalidTypeException e) {
			throw new UnexpectedPathException("currentDeserializationPath should be valid: \"" + currentPath + "\"", e);
		}
	}

	/**
	 * @return true if the given parameter is computed automatically during
	 * deserialization, and therefore does not appear in the serialized output.
	 */
	public static boolean isImplicitParameter(Class<?> nodeClass, Parameter parameter) {
		String name = parameter.getName();
		ParameterInfo info = infoFor(nodeClass);
		return info.annotatedParameters_Self.contains(name)
			|| info.annotatedParameters_Enclosing.contains(name);
	}


	private static ParameterInfo infoFor(Class<?> nodeClass) {
		return PARAMETER_INFO_MAP.computeIfAbsent(nodeClass, SerializationPlugin::computeInfoFor);
	}

	private static ParameterInfo computeInfoFor(Class<?> nodeClass) {
		Set<String> selfParameters = new HashSet<>();
		Set<String> enclosingParameters = new HashSet<>();
		Map<String, DeserializationPath> deserializationPathParameters = new HashMap<>();
		Map<String, Object> polyfills = new HashMap<>();
		AtomicReference<VariantCaseMapInfo> variantCaseMap = new AtomicReference<>(new NoVariantCaseMap(nodeClass));

		if (!nodeClass.isInterface()) { // Avoid for @VariantCaseMap classes
			for (Parameter parameter: ReferenceUtils.theOnlyConstructorFor(nodeClass).getParameters()) {
				scanForInfo(parameter, parameter.getName(),
					selfParameters, enclosingParameters, deserializationPathParameters, polyfills);
			}
		}

		// Bosk generally ignores an object's fields, looking only at its
		// constructor arguments and its getters. However, we make an exception
		// for convenience: Bosk annotations that go on constructor parameters
		// can also go on fields with the same name. This accommodates systems
		// like Lombok that derive constructors from fields.
		//
		// It's also required to scan static fields for features like @VariantCaseMap.

		for (Class<?> c = nodeClass; c != Object.class && c != null; c = c.getSuperclass()) {
			for (Field field: c.getDeclaredFields()) {
				scanForInfo(field, field.getName(),
					selfParameters, enclosingParameters, deserializationPathParameters, polyfills);
			}
		}

		if (VariantNode.class.isAssignableFrom(nodeClass)) {
			scanForVariantCaseMap(nodeClass, variantCaseMap);
		}

		return new ParameterInfo(selfParameters, enclosingParameters, deserializationPathParameters, polyfills, variantCaseMap.get());
	}

	@SuppressWarnings({"rawtypes","unchecked"})
	private static void scanForVariantCaseMap(Class<?> nodeClass, AtomicReference<VariantCaseMapInfo> variantCaseMap) {
		if (!VariantNode.class.isAssignableFrom(nodeClass)) {
			return;
		}

		for (Class<?> c = nodeClass; c != Object.class && c != null; c = c.getSuperclass()) {
			for (Field f: c.getDeclaredFields()) {
				var annotations = f.getAnnotationsByType(VariantCaseMap.class);
				if (annotations.length == 0) {
					// This is not the droid you're looking for
					continue;
				} else if (annotations.length >= 2) {
					throw new IllegalStateException("Multiple variant case maps for the same class: " + f);
				}
				if (!isStatic(f.getModifiers()) || isPrivate(f.getModifiers())) {
					throw new IllegalStateException("The variant case map must be static and final: " + f);
				}
				f.setAccessible(true);
				MapValue value;
				try {
					value = (MapValue) f.get(null);
				} catch (IllegalAccessException e) {
					throw new AssertionError("Field should not be inaccessible: " + f, e);
				}
				if (value == null) {
					throw new NullPointerException("VariantCaseMap cannot be null: " + f);
				}
				var old = variantCaseMap.get();
				boolean success = variantCaseMap.compareAndSet(old, old.plus(nodeClass, value, c));
				assert success: "Hey who's messing with our AtomicReference?";
			}
		}

		// Recurse to look for inherited variant case maps
		for (var i : nodeClass.getInterfaces()) {
			scanForVariantCaseMap(i, variantCaseMap);
		}
		Class<?> superclass = nodeClass.getSuperclass();
		if (superclass != null && superclass != Object.class) {
			scanForVariantCaseMap(superclass, variantCaseMap);
		}
	}

	private static void scanForInfo(AnnotatedElement thing, String name, Set<String> selfParameters, Set<String> enclosingParameters, Map<String, DeserializationPath> deserializationPathParameters, Map<String, Object> polyfills) {
		if (thing.isAnnotationPresent(Self.class)) {
			selfParameters.add(name);
		} else if (thing.isAnnotationPresent(Enclosing.class)) {
			enclosingParameters.add(name);
		} else if (thing.isAnnotationPresent(DeserializationPath.class)) {
			deserializationPathParameters.put(name, thing.getAnnotation(DeserializationPath.class));
		} else if (thing.isAnnotationPresent(Polyfill.class)) {
			if (thing instanceof Field f && isStatic(f.getModifiers()) && !isPrivate(f.getModifiers())) {
				f.setAccessible(true);
				for (Polyfill polyfill : thing.getAnnotationsByType(Polyfill.class)) {
					Object value;
					try {
						value = f.get(null);
					} catch (IllegalAccessException e) {
						throw new AssertionError("Field should not be inaccessible: " + f, e);
					}
					if (value == null) {
						throw new NullPointerException("Polyfill value cannot be null: " + f);
					}
					ANY_POLYFILLS.set(true);
					for (String fieldName: polyfill.value()) {
						Object previous = polyfills.put(fieldName, value);
						if (previous != null) {
							throw new IllegalStateException("Multiple polyfills for the same field \"" + fieldName + "\": " + f);
						}
					}
					// TODO: Polyfills can't be used for implicit refs, Optionals, Phantoms
					// Also can't be used for Entity.id
				}
			} else {
				throw new IllegalStateException("@Polyfill annotation is only valid on non-private static fields; found on " + thing);
			}
		}
	}

	private record ParameterInfo(
		Set<String> annotatedParameters_Self,
		Set<String> annotatedParameters_Enclosing,
		Map<String, DeserializationPath> annotatedParameters_DeserializationPath,
		Map<String, Object> polyfills,
		VariantCaseMapInfo variantCaseMap
	) { }

	private sealed interface VariantCaseMapInfo {
		/**
		 * We're just scanning for info here, not throwing exceptions.
		 * Hence, we simply record what we discovered, and then when anyone asks for the variant case map, <em>then</em> we throw.
		 */
		@Nullable MapValue<Type> ifAny() throws InvalidTypeException;

		/**
		 * @param nodeClass the class whose variant case map we're looking for
		 * @param map the variant case map we found
		 * @param origin the class in which we found it
		 */
		VariantCaseMapInfo plus(Class<?> nodeClass, MapValue<Type> map, Class<?> origin);
	}

	private record NoVariantCaseMap(Class<?> nodeClass) implements VariantCaseMapInfo {
		@Override public MapValue<Type> ifAny() { return null; }

		@Override
		public VariantCaseMapInfo plus(Class<?> nodeClass, MapValue<Type> map, Class<?> origin) {
			return new OneVariantCaseMap(map, origin);
		}
	}

	private record OneVariantCaseMap(MapValue<Type> map, Class<?> origin) implements VariantCaseMapInfo {
		@Override public MapValue<Type> ifAny() { return map; }

		@Override
		public VariantCaseMapInfo plus(Class<?> nodeClass, MapValue<Type> map, Class<?> origin) {
			return new AmbiguousVariantCaseMap(nodeClass, ConsPStack.<Class<?>>singleton(this.origin).plus(origin));
		}
	}

	private record AmbiguousVariantCaseMap(Class<?> nodeClass, ConsPStack<Class<?>> origins) implements VariantCaseMapInfo {
		@Override
		public MapValue<Type> ifAny() throws InvalidTypeException {
			throw new InvalidTypeException(nodeClass.getSimpleName() + " has multiple variant case maps in " + origins);
		}

		@Override
		public VariantCaseMapInfo plus(Class<?> nodeClass, MapValue<Type> map, Class<?> origin) {
			return new AmbiguousVariantCaseMap(nodeClass, this.origins.plus(origin));
		}
	}

	private static final Map<Class<?>, ParameterInfo> PARAMETER_INFO_MAP = new ConcurrentHashMap<>();
	private static final AtomicBoolean ANY_POLYFILLS = new AtomicBoolean(false);

	private static final Logger LOGGER = LoggerFactory.getLogger(SerializationPlugin.class);
}
