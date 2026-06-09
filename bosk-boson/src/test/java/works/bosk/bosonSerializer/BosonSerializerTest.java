package works.bosk.bosonSerializer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import works.bosk.Bosk;
import works.bosk.BoskConfig;
import works.bosk.BoskDriver;
import works.bosk.BoskDriver.EntireState;
import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.Entity;
import works.bosk.Identifier;
import works.bosk.Reference;
import works.bosk.SideTable;
import works.bosk.SideTableReference;
import works.bosk.StateTreeNode;
import works.bosk.annotations.ReferencePath;
import works.bosk.annotations.Self;
import works.bosk.boson.codec.Codec;
import works.bosk.boson.codec.CodecBuilder;
import works.bosk.boson.codec.io.CharArrayJsonReader;
import works.bosk.boson.mapping.TypeMap;
import works.bosk.boson.mapping.TypeScanner;
import works.bosk.boson.types.DataType;
import works.bosk.boson.types.TypeReference;
import works.bosk.exceptions.InvalidTypeException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BosonSerializerTest {

	public record Root(
		Catalog<Key> keys,
		Catalog<Item> items,
		SideTable<Key, String> sideTable,
		@Self Reference<Root> self
	) implements StateTreeNode {}

	public record Key(
		Identifier id
	) implements Entity {}

	public record Item(
		Identifier id,
		@Self Reference<Item> self
	) implements Entity {}

	public interface Refs {
		@ReferencePath("/keys") CatalogReference<Key> keys();
		@ReferencePath("/items") CatalogReference<Item> items();
		@ReferencePath("/sideTable") SideTableReference<Key, String> sideTable();
	}

	Bosk<Root> bosk;
	Refs refs;
	TypeMap typeMap;
	Codec codec;

	@BeforeEach
	void setup() throws InvalidTypeException {
		bosk = new Bosk<>(
			"test",
			Root.class,
			BosonSerializerTest::emptyState,
			BoskConfig.simple());
		refs = bosk.buildReferences(Refs.class);
		typeMap = new TypeScanner(TypeMap.Settings.DEFAULT)
			.addBundle(new BosonSerializer().bundleFor(bosk))
			.scan(DataType.of(Root.class))
			.build();
		codec = CodecBuilder.using(typeMap).buildInterpreter();
	}

	private static BoskDriver.EntireState.SingleTree<Root> emptyState(Bosk<Root> b) throws InvalidTypeException {
		return EntireState.just(emptyRoot(b));
	}

	private static @NonNull Root emptyRoot(Bosk<Root> b) throws InvalidTypeException {
		Refs refs = b.buildReferences(Refs.class);
		return new Root(
			Catalog.empty(),
			Catalog.empty(),
			SideTable.empty(refs.keys()),
			b.rootReference()
		);
	}

	@Test
	void sideTable() throws InvalidTypeException, IOException {
		var object = SideTable.of(refs.keys(), Identifier.from("key"), "value");


		var gen = codec.generatorFor(typeMap.get(DataType.of(refs.sideTable().targetType())));
		Writer stringWriter = new StringWriter();
		gen.generate(stringWriter, object);
		String json = stringWriter.toString();

		var parser = codec.parserFor(typeMap.get(DataType.of(refs.sideTable().targetType())));
		var parsed = parser.parse(new CharArrayJsonReader(json.toCharArray()));
		assertEquals(object, parsed);
	}

	@Test
	void reference() {
		var gen = codec.generatorFor(typeMap.get(DataType.of(new TypeReference<CatalogReference<Key>>(){})));
		Writer stringWriter = new StringWriter();
		gen.generate(stringWriter, refs.keys());
		String json = stringWriter.toString();
		assertEquals("\"/keys\"", json);
	}

	@Test
	void selfReference() throws IOException, InvalidTypeException {
		var parser = codec.parserFor(typeMap.get(DataType.of(Root.class)));
		Root parsed = (Root)parser.parse(new CharArrayJsonReader(
			"""
			{
				"keys": [],
				"items": [],
				"sideTable": {
					"domain": "/keys",
					"valuesById": []
				}
			}
			""".toCharArray()
		));
		assertEquals(emptyRoot(bosk), parsed);
	}

	@Test
	void catalogEntrySelfReference() throws IOException, InvalidTypeException {
		var itemId = Identifier.from("e1");
		var expectedItem = new Item(itemId, refs.items().then(itemId));
		var originalRoot = new Root(
			Catalog.empty(),
			Catalog.of(expectedItem),
			SideTable.empty(refs.keys()),
			bosk.rootReference()
		);

		var gen = codec.generatorFor(typeMap.get(DataType.of(Root.class)));
		StringWriter sw = new StringWriter();
		gen.generate(sw, originalRoot);
		String json = sw.toString();

		var parser = codec.parserFor(typeMap.get(DataType.of(Root.class)));
		Root parsedRoot = (Root)parser.parse(new CharArrayJsonReader(json.toCharArray()));

		assertEquals(originalRoot, parsedRoot);
	}
}
