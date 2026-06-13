package works.bosk.graphql;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import works.bosk.graphql.exceptions.UnsupportedNameException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.graphql.BoskGraphQL.typeName;

public class TypeNameTest {
	@ValueSource(classes = {
		void.class,
		byte.class,
		char.class,
		short.class,
		int.class,
		long.class,
		float.class,
		double.class,
		String.class,
		TestEnum.class,
		TypeNameTest.class
	})
	@ParameterizedTest
	void baseCases(Class<?> clazz) {
		assertEquals(clazz.getSimpleName(), typeName(clazz));
	}

	@Test
	void nestedGenerics() {
		record With_Underscore(){}
		record Holder(
			List<List<String>> list1,
			List<With_Underscore> list2,
			Map<String, List<With_Underscore>> map1,
			Map<Map<String, String>, List<Map<String, String>>> map2
		){}
		Type[] fields = Stream.of(Holder.class.getDeclaredFields())
			.map(Field::getGenericType)
			.toArray(Type[]::new);

		assertEquals("List_List_String", typeName(fields[0]));
		assertEquals("List_u1With_Underscore", typeName(fields[1]));
		assertEquals("Map_String_List_u1With_Underscore", typeName(fields[2]));
		assertEquals("Map_Map_String_String_List_Map_String_String", typeName(fields[3]));
	}

	@Test
	void sneakyNames() {
		record u3WithoutUnderscores() {}
		record u3With_Underscores() {}
		record Holder(
			List<u3WithoutUnderscores> field0,
			List<u3With_Underscores> field1
		){}
		assertEquals("List_u0u3WithoutUnderscores",
			typeName(Holder.class.getDeclaredFields()[0].getGenericType()));
		assertEquals("List_u1u3With_Underscores",
			typeName(Holder.class.getDeclaredFields()[1].getGenericType()));
	}

	@Test
	void badNames() {
		record _LeadingUnderscore(){}
		assertThrows(UnsupportedNameException.class,
			() -> typeName(_LeadingUnderscore.class));
	}

	enum TestEnum { ONE, TWO }

}
