package works.bosk.json.mapping;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;

public class ReflectionUtils {

	public record RecordInfo(
		List<RecordComponent> components,
		Map<String, MethodHandle> accessorsByName,
		MethodHandle constructor
	) { }

	public static RecordInfo recordInfo(Class<? extends Record> recordClass) {
		return RECORD_INFO.computeIfAbsent(recordClass, ReflectionUtils::computeRecordInfo);
	}

	private static RecordInfo computeRecordInfo(Class<? extends Record> recordClass) {
		RecordComponent[] components = recordClass.getRecordComponents();
		Map<String, MethodHandle> accessorsByName = new LinkedHashMap<>();
		for (var rc: components) {
			MethodHandle handle;
			try {
				handle = MethodHandles.lookup().unreflect(rc.getAccessor());
			} catch (IllegalAccessException e) {
				throw new IllegalStateException("Accessor for " + rc.getName() + " is not accessible for " + recordClass.getSimpleName(), e);
			}
			accessorsByName.put(rc.getName(), handle);
		}
		Constructor<? extends Record> ctor;
		try {
			ctor = recordClass.getConstructor(Stream.of(components).map(RecordComponent::getType).toArray(Class[]::new));
		} catch (NoSuchMethodException e) {
			throw new IllegalStateException("Can't find record constructor", e);
		}
		MethodHandle handle;
		try {
			handle = MethodHandles.lookup().unreflectConstructor(ctor);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Constructor is not accessible for " + recordClass.getSimpleName(), e);
		}
		return new RecordInfo(List.of(components), unmodifiableMap(accessorsByName), handle);
	}

	private static final ConcurrentHashMap<Class<? extends Record>, RecordInfo> RECORD_INFO = new ConcurrentHashMap<>();

}
