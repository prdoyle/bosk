package works.bosk.junit;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;

/**
 * Shared support for injection at method and class level.
 */
class InjectionSupport {
	static final ExtensionContext.Namespace NAMESPACE =
		ExtensionContext.Namespace.create(InjectionSupport.class);
	static final String BRANCH_KEY = "branch";

	/**
	 * Determine what branches are needed to provide all the injectors required
	 * directly or indirectly by the {@code requiredParameters}.
	 * <p>
	 * This is done in two phases.
	 * First, we compute all possible branches by instantiating all injector classes.
	 * Once we have the injectors, we can use {@link Injector#supportsParameter}
	 * to determine which ones are needed,
	 * and do a second pass to compute the branches for just those injectors.
	 *
	 * @see Branch
	 */
	static List<Branch> computeBranchesForParameters(ExtensionContext context, List<Parameter> requiredParameters, Branch startingBranch) {
		// We have a chicken-and-egg thing happening here:
		// we can't know which injectors we need until we call Injector.supportsParameter,
		// which we can't do until we've instantiated the injectors.
		//
		// So, begin by assuming we'll need them all:
		//
		List<Branch> allPossibleBranches = List.of(startingBranch);
		var allInjectorClasses = getAllInjectorClasses(context);
		for (var injectorClass : allInjectorClasses) {
			allPossibleBranches = expandedBranches(allPossibleBranches, injectorClass);
		}

		if (allPossibleBranches.isEmpty()) {
			// TODO: Is this right? Shouldn't it be List.of(startingBranch)?
			return List.of();
		}

		// At this stage, we have a list of branches that have instantiated
		// all the injectors we could possibly have needed for any parameter,
		// but some injectors might be for parameters that aren't actually used
		// by the test method or the class constructor.
		//
		// If we don't prune out the unneeded injectors,
		// we will end up calling the test method with the same parameters
		// multiple times, varying only the values of parameters
		// that aren't even used.
		//
		// Let's determine which injector classes we actually needed.
		// We can do this by picking any Branch (they all have the same types of injectors)
		// and seeing which injectors were needed
		// to provide values for the requiredParameters.

		var neededInjectorClasses = new HashSet<Class<? extends Injector>>();
		Branch someBranch = allPossibleBranches.getFirst();
		requiredParameters.forEach(p -> {
			var key = someBranch.keyForParameter(p);
			if (key != null && neededInjectorClasses.add(key.injector().getClass())) {
				neededInjectorClasses.addAll(someBranch.toInject.get(key).provenance());
			}
		});

		// And now we can recalculate the branches a second time,
		// expanding only the injector classes known to be needed.
		List<Branch> neededBranches = List.of(startingBranch);

		// The order matters here because later injector classes can have
		// constructor parameters injected from earlier ones.
		for (var injectorClass : allInjectorClasses) {
			if (neededInjectorClasses.contains(injectorClass)) {
				// Only expand if this injector class is NOT already in startingBranch
				boolean alreadyInStarting = startingBranch.toInject().keySet()
					.stream().anyMatch(key -> key.injector().getClass() == injectorClass);

				if (!alreadyInStarting) {
					neededBranches = expandedBranches(neededBranches, injectorClass);
				}
			}
		}
		return neededBranches;
	}


	/**
	 * Simpler than {@link #computeBranchesForParameters(ExtensionContext, List, Branch) computeBrancesForParameters}
	 * because there's no particular requirement for precision at the class level:
	 * we can rely on {@code computeBranchesForParameters} to clean up
	 * unnecessary branches for us.
	 * <p>
	 * TODO: Is this true? What about a class that has no parameter injection
	 *  and also has an injector that might have been needed but actually is not.
	 *  Don't we end up with the same problem as in {@code computeBranchesForParameters}?
	 */
	static List<Branch> computeBranchesForFields(ExtensionContext context) {
		var injectedFields = getInjectedFields(context);

		// Class-level branches start from scratch because, unlike for
		// method-level (parameter) branches, there's no higher-level
		// branch computation context to build from.
		List<Branch> branches = List.of(Branch.empty());
		for (var injectorClass : getAllInjectorClasses(context)) {
			if (mightSupportField(injectorClass, injectedFields)) {
				branches = expandedBranches(branches, injectorClass);
			}
		}
		return branches;
	}

	/**
	 * @return the injector classes in the order they should be instantiated
	 */
	static List<Class<? extends Injector>> getAllInjectorClasses(ExtensionContext context) {
		List<Class<?>> bottomUp = new ArrayList<>();
		for (var c = context.getRequiredTestClass(); c != Object.class; c = c.getSuperclass()) {
			bottomUp.add(c);
		}
		List<Class<? extends Injector>> allInjectors = new ArrayList<>();
		for (var c : bottomUp.reversed()) {
			for (var a: c.getAnnotationsByType(InjectFrom.class)) {
				allInjectors.addAll(asList(a.value()));
			}
		}
		return allInjectors;
	}

	/**
	 * A version of {@link Branch#withInjectors(Class)} that operates on a list.
	 */
	private static List<Branch> expandedBranches(List<Branch> currentBranches, Class<? extends Injector> injectorType) {
		List<Branch> expanded = new ArrayList<>();
		for (Branch branch : currentBranches) {
			expanded.addAll(branch.withInjectors(injectorType));
		}
		return unmodifiableList(expanded);
	}

	/**
	 * A possible future in which certain values are chosen for injection.
	 * <p>
	 * Because parameters can be injected into the injectors themselves,
	 * the parameters are not fully independent of each other,
	 * and so a straightforward cartesian product of all parameter values doesn't work.
	 * A {@code Branch} represents one "scenario" for the injectors,
	 * within which the full cartesian product expansion of parameter values is valid.
	 * <p>
	 * Uses a quantum "many worlds" metaphor to describe possible futures.
	 * Specifically: when one injector injects into another,
	 * we will need multiple instances of the latter injector,
	 * and that is what leads to multiple branches.
	 * On each branch, the providing injector injects a single value,
	 * so "the wavefunction has collapsed" on that branch,
	 * and the providing injector is treated as though it provided just a single value.
	 * <p>
	 * During the instantiation of injectors, the branch may be "incomplete" in the sense
	 * that it contains entries for only the first N {@code InjectionKey}s.
	 *
	 * @param toInject A map from each {@link InjectionKey} to the list of values associated with that key on this branch.
	 *                 For cases where an injector has already had its constructor parameters supplied by earlier injectors,
	 *                 the map will contain just the single value used to construct that injector on this branch.
	 */
	record Branch(
		Map<InjectionKey, Superposition> toInject
	) {
		static Branch empty() {
			return new Branch(Map.of());
		}

		List<Branch> withInjectors(Class<? extends Injector> injectorType) {
			Constructor<?>[] ctors = injectorType.getDeclaredConstructors();
			if (ctors.length != 1) {
				throw new ParameterResolutionException("Injector class must have exactly one constructor: " + injectorType);
			}
			var ctor = ctors[0];
			setAccessible(ctor);
			List<InjectionKey> keysToUse = Arrays.stream(ctor.getParameters())
				.map(this::keyForParameter)
				.filter(Objects::nonNull)
				.distinct()
				.toList();

			List<? extends List<?>> valueLists = keysToUse.stream()
				.map(toInject::get)
				.map(Superposition::values)
				.toList();

			List<Branch> result = new ArrayList<>();
			for (List<Object> combos : cartesianProduct(valueLists)) {
				try {
					List<Object> args = new ArrayList<>();
					var keysUsed = new LinkedHashSet<InjectionKey>();
					for (var p: ctor.getParameters()) {
						var pi = keyForParameter(p);
						keysUsed.add(pi);
						var index = keysToUse.indexOf(pi);
						assert index >= 0: "Internal error: injector not found for parameter " + p + " of constructor " + ctor;
						args.add(combos.get(index));
					}

					var injector = (Injector) ctor.newInstance(args.toArray());

					var provenance = new HashSet<Class<? extends Injector>>();
					keysUsed.forEach(key -> {
						provenance.add(key.injector().getClass());
						provenance.addAll(toInject.get(key).provenance());
					});
					provenance.add(injector.getClass());

					var map = new LinkedHashMap<>(this.toInject);
					map.put(new InjectionKey(injector), new Superposition(injector.values(), provenance));

					int i = 0;
					for (var pi: keysToUse) {
						var possibleValues = toInject.get(pi).values();
						if (possibleValues.size() >= 2) {
							map.put(pi, map.get(pi).collapsed(args.get(i)));
						}
						++i;
					}
					result.add(new Branch(map));
				} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
					throw new ParameterResolutionException("Error calling constructor on injector class " + injectorType, e);
				}
			}
			return result;
		}

		@Nullable
		InjectionKey keyForParameter(Parameter p) {
			return keyFor(p, p.getType());
		}

		@Nullable
		InjectionKey keyForField(Field f) {
			return keyFor(f, f.getType());
		}

		@Nullable
		InjectionKey keyFor(AnnotatedElement element, Class<?> elementType) {
			return List.copyOf(toInject.entrySet())
				.reversed()
				.stream()
				.filter(e -> e.getKey().injector().supports(element, elementType))
				.findFirst()
				.map(Map.Entry::getKey)
				.orElse(null);
		}

		Branch withFieldValues(Map<Field, Object> fieldValues) {
			var newMap = new LinkedHashMap<>(toInject);
			for (var entry : fieldValues.entrySet()) {
				var key = keyForField(entry.getKey());
				Superposition existing = toInject.get(key);
				newMap.put(key, existing.collapsed(entry.getValue()));
			}
			return new Branch(newMap);
		}

		@Override
		public String toString() {
			return toInject.entrySet().stream()
				.map(e -> e.getKey().injector().getClass().getSimpleName() + "=" + e.getValue().values())
				.collect(joining(", ", "Branch{", "}"));
		}
	}

	/**
	 * Identifies a particular collection of values to be injected by a particular injector.
	 * Two fields/parameters that use the same {@code InjectionKey} will always receive the same value;
	 * those with different {@code InjectionKey}s will receive combinations of values.
	 *
	 * @param injector the injector instance providing values for this key
	 */
	record InjectionKey(Injector injector) { }

	/**
	 * Represents the possible values to be injected for a particular {@link InjectionKey}
	 * on a particular {@link Branch}.
	 * In other words: given how injection decisions already made earlier on the
	 * branch affect constructor parameters of other injectors,
	 * this represents the values to be injected for a particular {@link InjectionKey}.
	 *
	 * @param values the subset of {@link Injector#values()} to be injected in this scenario
	 * @param provenance the set of injector classes required, directly or indirectly,
	 *                   to produce these values, with no guarantees on the order
	 */
	record Superposition(
		List<?> values,
		Set<Class<? extends Injector>> provenance
	){
		Superposition collapsed(Object singleValue) {
			return new Superposition(List.of(singleValue), provenance);
		}
	}

	@SuppressForbidden("Only for testing code; we have few other options here")
	static void setAccessible(AccessibleObject accessibleObject) {
		accessibleObject.setAccessible(true);
	}

	/**
	 * Compute the cartesian product of a list of lists.
	 */
	static List<List<Object>> cartesianProduct(Collection<? extends List<?>> input) {
		List<List<Object>> result = List.of(List.of());
		for (List<?> list : input) {
			result = result.stream()
				.flatMap(prev -> list.stream().map(v -> {
					List<Object> next = new ArrayList<>(prev);
					next.add(v);
					return next;
				}))
				.toList();
		}
		return result;
	}

	/**
	 * @return true if the injector class supports at least one of the given fields.
	 * <p>
	 * Note: This method cannot determine field support for injectors with constructor dependencies,
	 * since we don't have the values for injector constructor parameters available at this point.
	 * In such cases, we conservatively return true if the fields list is non-empty,
	 * allowing the injector to be instantiated and checked later.
	 */
	static boolean mightSupportField(Class<? extends Injector> injectorClass, List<Field> fields) {
		try {
			var injector = injectorClass.getDeclaredConstructors()[0];
			setAccessible(injector);
			var params = injector.getParameters();

			if (params.length > 0) {
				// Can't instantiate without dependencies, so assume it might support fields
				return !fields.isEmpty();
			}
			var injectorInstance = (Injector) injector.newInstance();
			for (Field f : fields) {
				if (injectorInstance.supportsField(f)) {
					return true;
				}
			}
			return false;
		} catch (Exception e) {
			throw new ParameterResolutionException("Error checking field support for " + injectorClass, e);
		}
	}

	/**
	 * @return the fields annotated with {@link Injected} in the class hierarchy
	 */
	static List<Field> getInjectedFields(ExtensionContext context) {
		List<Field> fields = new ArrayList<>();
		for (var c = context.getRequiredTestClass(); c != Object.class; c = c.getSuperclass()) {
			for (var field : c.getDeclaredFields()) {
				if (field.isAnnotationPresent(Injected.class)) {
					fields.add(field);
				}
			}
		}
		return fields;
	}

}
