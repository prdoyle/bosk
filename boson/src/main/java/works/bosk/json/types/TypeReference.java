package works.bosk.json.types;

public abstract class TypeReference<T> {
	java.lang.reflect.Type reflectionType() {
		return ((java.lang.reflect.ParameterizedType) getClass()
			.getGenericSuperclass()).getActualTypeArguments()[0];
	}
}
