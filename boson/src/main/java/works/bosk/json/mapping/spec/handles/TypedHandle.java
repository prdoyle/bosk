package works.bosk.json.mapping.spec.handles;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.WrongMethodTypeException;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import works.bosk.json.types.DataType;
import works.bosk.json.types.InstanceType;
import works.bosk.json.types.KnownType;

import static java.lang.invoke.MethodHandles.insertArguments;
import static java.util.Objects.requireNonNull;

public record TypedHandle(
	MethodHandle handle,
	KnownType returnType,
	List<KnownType> parameterTypes
) {
	public TypedHandle {
		requireNonNull(returnType);
		requireNonNull(parameterTypes);
		requireNonNull(handle);
		MethodType equivalentMethodType = equivalentMethodType(returnType, parameterTypes);
		if (!handle.type().equals(equivalentMethodType)) {
			System.err.println("OH NOES");
		}
		assert handle.type().equals(equivalentMethodType(returnType, parameterTypes));
	}

	private static MethodType equivalentMethodType(KnownType returnType, List<KnownType> parameterTypes) {
		return MethodType.methodType(
			returnType.rawClass(),
			parameterTypes.stream().map(KnownType::rawClass).toArray(Class<?>[]::new)
		);
	}

	public Object invoke(Object... args) {
		try {
			return handle.invokeWithArguments(args);
		} catch (ClassCastException | WrongMethodTypeException e) {
			throw new IllegalStateException("Unexpected type error invoking method handle " + this, e);
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}

	public TypedHandle curry(int parameterIndex, Object value) {
		if (parameterIndex < 0 || parameterIndex >= parameterTypes.size()) {
			throw new IndexOutOfBoundsException("No parameter " + parameterIndex + " in " + this);
		}
		MethodHandle newHandle = insertArguments(handle, parameterIndex, value);
		List<KnownType> newParameterTypes = new ArrayList<>(parameterTypes);
		newParameterTypes.remove(parameterIndex);
		return new TypedHandle(newHandle, returnType, List.copyOf(newParameterTypes));
	}

	public TypedHandle bind(int parameterIndex, TypedHandle parameter) {
		// Drop the formality: cast the parameter handle to the expected type
		MethodHandle parameterHandle = parameter.handle()
			.asType(parameter.handle.type()
				.changeReturnType(handle.type().parameterType(parameterIndex)));
		var resultHandle = MethodHandles.collectArguments(handle, parameterIndex, parameterHandle);
		List<KnownType> resultParameterTypes = new ArrayList<>(parameterTypes());
		resultParameterTypes.remove(parameterIndex);
		resultParameterTypes.addAll(parameter.parameterTypes());
		return new TypedHandle(resultHandle, returnType, resultParameterTypes);
	}

	@Override
	public String toString() {
		return "(" + String.join(", ", parameterTypes.stream().map(KnownType::toString).toList()) + ")->" + returnType;
	}

	public static TypedHandle ofRunnable(Runnable runnable) {
		return new TypedHandle(
			RUNNABLE_RUN.bindTo(runnable),
			DataType.VOID,
			List.of()
		);
	}

	public static <R> TypedHandle ofSupplier(Supplier<R> supplier) {
		var functionType = (InstanceType)DataType.known(supplier.getClass());
		var returnType = functionType.parameterBinding(Supplier.class, 0);
		return new TypedHandle(
			SUPPLIER_GET.bindTo(supplier),
			returnType.knownType(),
			List.of()
		);
	}

	public static <T> TypedHandle ofCallable(Callable<T> callable) {
		var functionType = (InstanceType)DataType.known(callable.getClass());
		var argType = functionType.parameterBinding(Consumer.class, 0);
		return new TypedHandle(
			CALLABLE_CALL.bindTo(callable),
			DataType.VOID,
			List.of(argType.knownType())
		);
	}

	public static <T,R> TypedHandle ofFunction(Function<T,R> function) {
		var functionType = (InstanceType)DataType.known(function.getClass());
		var argType = functionType.parameterBinding(Function.class, 0);
		var returnType = functionType.parameterBinding(Function.class, 1);
		return new TypedHandle(
			FUNCTION_APPLY.bindTo(function),
			returnType.knownType(),
			List.of(argType.knownType())
		);
	}

	public static <T> TypedHandle ofPredicate(Predicate<T> predicate) {
		var functionType = (InstanceType)DataType.known(predicate.getClass());
		var argType = functionType.parameterBinding(Predicate.class, 0);
		return new TypedHandle(
			PREDICATE_TEST.bindTo(predicate),
			DataType.BOOLEAN,
			List.of(argType.knownType())
		);
	}

	public static TypedHandle ofComponentAccessor(RecordComponent rc) {
		MethodHandle handle;
		try {
			handle = MethodHandles.lookup().unreflect(rc.getAccessor());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
		KnownType componentType = DataType.known(rc.getType());
		KnownType recordType = DataType.known(rc.getDeclaringRecord());
		return new TypedHandle(handle, componentType, List.of(recordType));
	}

	private static final MethodHandle RUNNABLE_RUN, FUNCTION_APPLY, PREDICATE_TEST, SUPPLIER_GET, CALLABLE_CALL;

	static {
		try {
			RUNNABLE_RUN = MethodHandles.lookup().findVirtual(Runnable.class, "run", MethodType.methodType(void.class));
			FUNCTION_APPLY = MethodHandles.lookup().findVirtual(Function.class, "apply", MethodType.methodType(Object.class, Object.class));
			PREDICATE_TEST = MethodHandles.lookup().findVirtual(Predicate.class, "test", MethodType.methodType(boolean.class, Object.class));
			SUPPLIER_GET = MethodHandles.lookup().findVirtual(Supplier.class, "get", MethodType.methodType(Object.class));
			CALLABLE_CALL = MethodHandles.lookup().findVirtual(Callable.class, "call", MethodType.methodType(Object.class));
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
