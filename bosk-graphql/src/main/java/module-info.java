import works.bosk.graphql.BoskGraphQL;

/**
 * Generates a GraphQL schema from a Bosk state tree.
 * <p>
 * See {@link BoskGraphQL} for usage.
 */
module works.bosk.graphql {
	requires transitive com.graphqljava;
	requires transitive works.bosk.core;
	requires org.jspecify;

	exports works.bosk.graphql;
	exports works.bosk.graphql.exceptions;
}
