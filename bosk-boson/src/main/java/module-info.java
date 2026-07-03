/**
 * Serializer implementation that uses the Boson library to convert Bosk objects to and from JSON.
 */
module works.bosk.bosonSerializer {
	requires transitive works.bosk.core;
	requires transitive works.bosk.boson;
	requires static transitive org.jspecify;
	exports works.bosk.bosonSerializer;
}
