package works.bosk.junit;

import java.lang.reflect.AnnotatedElement;
import java.util.HashSet;
import java.util.List;

/**
 * Provides a series of possible values for a field or method parameter.
 * <p>
 * Note that these will be added to a {@link HashSet}.
 * Since only one injector of each class is in use at a time,
 * the default {@link Object#equals equals} and {@link Object#hashCode hashCode}
 * behaviour will work fine,
 * but if you override them, strange behaviour may result;
 * for example, parts of the injection logic may be unable to distinguish
 * two different injectors.
 *
 * @see InjectedTest
 * @see InjectFrom
 */
public interface Injector {
	/**
	 * @return the class as reported to {@link InjectFrom}.
	 * For most injectors, that's just the object's class,
	 * but for other kinds of supported injectors,
	 * like enums, it can be something else.
	 */
	default Class<?> injectorClass() { return getClass(); }

	/**
	 * Note: if this method returns different results for the same element
	 * at different times, strange behaviour may result.
	 * @return true if this injector provides values for the given element.
	 */
	boolean supports(AnnotatedElement element, Class<?> elementType);

	/**
	 * @return non-null {@link List} of values for the given parameter.
	 */
	List<?> values();

}
