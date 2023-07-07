package io.vena.bosk.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import io.vena.bosk.AbstractBoskTest;
import io.vena.bosk.Bosk;
import io.vena.bosk.Catalog;
import io.vena.bosk.CatalogReference;
import io.vena.bosk.Identifier;
import io.vena.bosk.ListValue;
import io.vena.bosk.Listing;
import io.vena.bosk.ListingEntry;
import io.vena.bosk.Path;
import io.vena.bosk.Reference;
import io.vena.bosk.SideTable;
import io.vena.bosk.TestEntityBuilder;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.UnexpectedPathException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static io.vena.bosk.AbstractBoskTest.TestEnum.OK;
import static io.vena.bosk.ListingEntry.LISTING_ENTRY;
import static io.vena.bosk.util.Types.parameterizedType;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GsonPluginTest extends AbstractBoskTest {
	private Bosk<TestRoot> bosk;
	private TestEntityBuilder teb;
	private Gson boskGson;
	private CatalogReference<TestEntity> entitiesRef;

	/**
	 * Not configured by GsonPlugin. Only for checking the properties of the generated JSON.
	 */
	private Gson plainGson;

	@BeforeEach
	void setUpGson() throws Exception {
		bosk = setUpBosk(Bosk::simpleDriver);
		teb = new TestEntityBuilder(bosk);
		entitiesRef = bosk.catalogReference(TestEntity.class, Path.just(TestRoot.Fields.entities));

		plainGson = new GsonBuilder()
				.setPrettyPrinting()
				.create();

		TypeAdapterFactory typeAdapterFactory = new GsonPlugin().adaptersFor(bosk);
		boskGson = new GsonBuilder()
			.registerTypeAdapterFactory(typeAdapterFactory)
			.excludeFieldsWithoutExposeAnnotation()
			.setPrettyPrinting()
			.create();
	}

	@ParameterizedTest
	@MethodSource("catalogArguments")
	void testToJson_catalog(List<String> ids) {
		// Build entities and put them in a Catalog
		List<TestEntity> entities = new ArrayList<>();
		for (String id : ids) {
			entities.add(teb.blankEntity(Identifier.from(id), OK));
		}
		Catalog<TestEntity> catalog = Catalog.of(entities);

		// Build the expected JSON structure
		List<Map<String, Object>> expected = new ArrayList<>();
		entities.forEach(e1 -> expected.add(singletonMap(e1.id().toString(), plainObjectFor(e1, e1.getClass()))));

		assertGsonWorks(expected, catalog, new TypeToken<Catalog<TestEntity>>(){}.getType());
	}

	static Stream<Arguments> catalogArguments() {
		return Stream.of(
				catalogCase(),
				catalogCase("1", "3", "2")
		);
	}

	private static Arguments catalogCase(String ...ids) {
		return Arguments.of(asList(ids));
	}

	@ParameterizedTest
	@MethodSource("listingArguments")
	void testToJson_listing(List<String> strings, List<Identifier> ids) {
		Listing<TestEntity> listing = Listing.of(entitiesRef, ids);

		Map<String, Object> expected = new LinkedHashMap<>();
		expected.put("ids", strings);
		expected.put("domain", entitiesRef.pathString());

		assertGsonWorks(expected, listing, new TypeToken<Listing<TestEntity>>(){}.getType());
	}

	static Stream<Arguments> listingArguments() {
		return Stream.of(
			listingCase(),
			listingCase("1", "3", "2")
		);
	}

	private static Arguments listingCase(String ...strings) {
		return Arguments.of(asList(strings), Stream.of(strings).map(Identifier::from).collect(toList()));
	}

	@Test
	void testListingEntry() {
		assertEquals("true", boskGson.toJson(LISTING_ENTRY));
		assertEquals(LISTING_ENTRY, boskGson.fromJson("true", ListingEntry.class));
	}

	@ParameterizedTest
	@MethodSource("sideTableArguments")
	void testToJson_sideTable(List<String> keys, Map<String,String> valuesByString, Map<Identifier, String> valuesById) {
		SideTable<TestEntity, String> sideTable = SideTable.fromOrderedMap(entitiesRef, valuesById);

		List<Map<String, Object>> expectedList = new ArrayList<>();
		valuesByString.forEach((key, value) -> expectedList.add(singletonMap(key, value)));

		Map<String, Object> expected = new LinkedHashMap<>();
		expected.put("valuesById", expectedList);
		expected.put("domain", entitiesRef.pathString());

		assertGsonWorks(
			expected,
			sideTable,
			new TypeToken<SideTable<TestEntity, String>>(){}.getType()
		);
	}

	static Stream<Arguments> sideTableArguments() {
		return Stream.of(
				sideTableCase(f->{}),
				sideTableCase(f->{
					f.accept("1", "First");
					f.accept("3", "Second");
					f.accept("2", "Third");
				})
		);
	}

	static <V> Arguments sideTableCase(Consumer<BiConsumer<String,V>> initializer) {
		Map<String,V> valuesByString = new LinkedHashMap<>();
		initializer.accept(valuesByString::put);

		Map<Identifier, V> valuesById = new LinkedHashMap<>();
		initializer.accept((k,v) -> valuesById.put(Identifier.from(k), v));

		List<String> keys = new ArrayList<>();
		initializer.accept((k,v)-> keys.add(k));
		return Arguments.of(keys, valuesByString, valuesById);
	}

	@Test
	void testPhantomIsOmitted() throws InvalidTypeException {
		TestEntity entity = makeEntityWithOptionalString(Optional.empty());
		String json = boskGson.toJson(entity);
		assertThat(json, not(containsString(Phantoms.Fields.phantomString)));
	}

	@Test
	void testOptionalIsOmitted() throws InvalidTypeException {
		TestEntity entity = makeEntityWithOptionalString(Optional.empty());
		String json = boskGson.toJson(entity);
		assertThat(json, not(containsString(Optionals.Fields.optionalString)));
	}

	@Test
	void testOptionalIsIncluded() throws InvalidTypeException {
		String contents = "OPTIONAL STRING CONTENTS";
		TestEntity entity = makeEntityWithOptionalString(Optional.of(contents));
		String json = boskGson.toJson(entity);
		assertThat(json, containsString(Optionals.Fields.optionalString));
		assertThat(json, containsString(contents));
	}

	@Test
	void testRootReference() {
		String json = boskGson.toJson(bosk.rootReference());
		assertEquals("\"/\"", json);
	}

	@ParameterizedTest
	@MethodSource("listValueArguments")
	void testToJson_listValue(List<?> list, TypeToken<?> typeToken) {
		ListValue<?> listValue = ListValue.from(list);
		String expected = plainGson.toJson(list);
		assertEquals(expected, boskGson.toJson(listValue, typeToken.getType()));
	}

	@ParameterizedTest
	@MethodSource("listValueArguments")
	void testFromJson_listValue(List<?> list, TypeToken<?> typeToken) {
		ListValue<?> expected = ListValue.from(list);
		String json = plainGson.toJson(list);
		Object actual = boskGson.fromJson(json, typeToken.getType());
		assertEquals(expected, actual);
		assertTrue(actual instanceof ListValue);
	}

	private static Stream<Arguments> listValueArguments() {
		return Stream.of(
			listValueCase(String.class),
			listValueCase(String.class, "Hello"),
			listValueCase(String.class, "first", "second")
			/*
			TODO: We can't yet handle parameterized node types!
			Can't tell that inside NodeWithGenerics<Double, Integer> the field listOfA has type ListValue<Double>.
			We currently don't do parameter substitution on type variables.

			listValueCase(
				parameterizedType(NodeWithGenerics.class, Double.class, Integer.class),
				new NodeWithGenerics<>(ListValue.of(1.0, 2.0), ListValue.of(3, 4)))
			 */
		);
	}

	private static Arguments listValueCase(Type entryType, Object...entries) {
		return Arguments.of(asList(entries), TypeToken.getParameterized(ListValue.class, entryType));
	}

	private TestEntity makeEntityWithOptionalString(Optional<String> optionalString) throws InvalidTypeException {
		CatalogReference<TestEntity> catalogRef = entitiesRef;
		Identifier entityID = Identifier.unique("testOptional");
		Reference<TestEntity> entityRef = catalogRef.then(entityID);
		CatalogReference<TestChild> childrenRef = entityRef.thenCatalog(TestChild.class, TestEntity.Fields.children);
		return new TestEntity(entityID, entityID.toString(), OK, Catalog.empty(), Listing.empty(childrenRef), SideTable.empty(childrenRef),
				Phantoms.empty(Identifier.unique("phantoms")),
				new Optionals(Identifier.unique("optionals"), optionalString, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
	}

	private void assertGsonWorks(Map<String,?> plainObject, Object boskObject, Type boskObjectType) {
		Map<String, Object> actualPlainObject = plainObjectFor(boskObject, boskObjectType);
		assertEquals(plainObject, actualPlainObject, "Serialized object should match expected");

		Object deserializedBoskObject = boskGsonObjectFor(plainObject, boskObjectType);
		assertEquals(boskObject, deserializedBoskObject, "Deserialized object should match expected");

		Map<String, Object> roundTripPlainObject = plainObjectFor(deserializedBoskObject, boskObjectType);
		assertEquals(plainObject, roundTripPlainObject, "Round-trip serialized object should match expected");

	}

	private void assertGsonWorks(List<?> plainList, Object boskObject, Type boskObjectType) {
		List<Object> actualPlainList = plainListFor(boskObject, boskObjectType);
		assertEquals(plainList, actualPlainList, "Serialized object should match expected");

		Object deserializedBoskObject = boskGsonListFor(plainList, boskObjectType);
		assertEquals(boskObject, deserializedBoskObject, "Deserialized object should match expected");

		List<Object> roundTripPlainObject = plainListFor(deserializedBoskObject, boskObjectType);
		assertEquals(plainList, roundTripPlainObject, "Round-trip serialized object should match expected");

	}

	private Map<String, Object> plainObjectFor(Object bsonObject, Type bsonObjectType) {
		String json = boskGson.toJson(bsonObject, bsonObjectType);
		return plainGson.fromJson(json, parameterizedType(Map.class, String.class, Object.class));
	}

	private List<Object> plainListFor(Object bsonObject, Type bsonObjectType) {
		String json = boskGson.toJson(bsonObject, bsonObjectType);
		return plainGson.fromJson(json, parameterizedType(List.class, Object.class));
	}

	private Object boskGsonObjectFor(Map<String, ?> plainObject, Type bsonObjectType) {
		String json = plainGson.toJson(plainObject, new TypeToken<Map<String, Object>>(){}.getType());
		return boskGson.fromJson(json, bsonObjectType);
	}

	private Object boskGsonListFor(List<?> plainList, Type bsonListType) {
		String json = plainGson.toJson(plainList, new TypeToken<List<Object>>(){}.getType());
		return boskGson.fromJson(json, bsonListType);
	}

	// Sad paths

	@Test
	void testBadJson_badReference() {
		assertThrows(UnexpectedPathException.class, () ->
			boskGson.fromJson("\"/some/nonexistent/path\"", parameterizedType(Reference.class, String.class)));
	}

	@Test
	void testBadJson_catalogFromEmptyMap() {
		assertJsonException("{}", Catalog.class, TestEntity.class);
	}

	@Test
	void testBadJson_catalogWithContentsArray() {
		assertJsonException("{ \"contents\": [] }", Catalog.class, TestEntity.class);
	}

	@Test
	void testBadJson_listingWithNoCatalog() {
		assertJsonException("{ \"ids\": [] }", Listing.class, TestEntity.class);
	}

	@Test
	void testBadJson_listingWithNoIds() {
		assertJsonException("{ \"domain\": \"/entities\" }", Listing.class, TestEntity.class);
	}

	@Test
	void testBadJson_listingWithExtraneousField() {
		assertJsonException("{ \"domain\": \"/entities\", \"extraneous\": 0, \"ids\": [] }", Listing.class, TestEntity.class);
	}

	@Test
	void testBadJson_listingWithTwoDomains() {
		assertJsonException("{ \"domain\": \"/entities\", \"domain\": \"/entities\", \"ids\": [] }", Listing.class, TestEntity.class);
	}

	@Test
	void testBadJson_listingWithTwoIdsFields() {
		assertJsonException("{ \"domain\": \"/entities\", \"ids\": [], \"ids\": [] }", Listing.class, TestEntity.class);
	}

	@Test
	void testBadJson_sideTableWithNoDomain() {
		assertJsonException("{ \"valuesById\": [] }", SideTable.class, TestEntity.class, String.class);
	}

	@Test
	void testBadJson_sideTableWithNoValues() {
		assertJsonException("{ \"domain\": \"/entities\" }", SideTable.class, TestEntity.class, String.class);
	}

	@Test
	void testBadJson_sideTableWithExtraneousField() {
		assertJsonException("{ \"domain\": \"/entities\", \"valuesById\": [], \"extraneous\": 0 }", SideTable.class, TestEntity.class, String.class);
	}

	@Test
	void testBadJson_sideTableWithTwoDomains() {
		assertJsonException("{ \"domain\": \"/entities\", \"domain\": \"/entities\", \"valuesById\": [] }", SideTable.class, TestEntity.class, String.class);
	}

	@Test
	void testBadJson_sideTableWithValuesMap() {
		assertJsonException("{ \"domain\": \"/entities\", \"valuesById\": {} }", SideTable.class, TestEntity.class, String.class);
	}

	@Test
	void testBadJson_sideTableWithTwoValuesFields() {
		assertJsonException("{ \"domain\": \"/entities\", \"valuesById\": [], \"valuesById\": [] }", SideTable.class, TestEntity.class, String.class);
	}

	private void assertJsonException(String json, Class<?> rawClass, Type... parameters) {
		assertThrows(JsonParseException.class, () -> boskGson.fromJson(json, parameterizedType(rawClass, parameters)));
	}

}
