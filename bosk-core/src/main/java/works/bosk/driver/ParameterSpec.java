package works.bosk.driver;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.List;

public record ParameterSpec(
	String name,
	Type type,
	List<Annotation> annotations
) {
	public static ParameterSpec of(Parameter p) {
		return new ParameterSpec(
			p.getName(),
			p.getParameterizedType(),
			List.of(p.getAnnotations())
		);
	}
}
