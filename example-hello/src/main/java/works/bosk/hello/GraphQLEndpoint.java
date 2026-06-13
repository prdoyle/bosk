package works.bosk.hello;

import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.analysis.MaxQueryComplexityInstrumentation;
import graphql.analysis.MaxQueryDepthInstrumentation;
import graphql.execution.instrumentation.ChainedInstrumentation;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import works.bosk.graphql.BoskGraphQL;

@RestController
@RequestMapping("/graphql")
public class GraphQLEndpoint {
	private final GraphQL graphQL;
	private final HelloBosk bosk;

	public GraphQLEndpoint(HelloBosk bosk) {
		this.bosk = bosk;
		this.graphQL = GraphQL.newGraphQL(BoskGraphQL.schemaFor(bosk))
			.instrumentation(new ChainedInstrumentation(List.of(
				new MaxQueryDepthInstrumentation(20),
				new MaxQueryComplexityInstrumentation(1000))))
			.build();
	}

	public record GraphQLRequest(
		String query,
		Map<String, Object> variables,
		String operationName
	) {
		public GraphQLRequest {
			if (query == null || query.isBlank()) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Missing or empty 'query' in request body");
			}
			if (variables == null) {
				variables = Map.of();
			}
		}
	}

	@PostMapping
	Map<String, Object> query(@RequestBody GraphQLRequest request) {
		try (var _ = bosk.readSession()) {
			var state = bosk.rootReference().valueIfExists();
			var input = ExecutionInput.newExecutionInput()
				.query(request.query())
				.variables(request.variables())
				.operationName(request.operationName())
				.root(state)
				.build();
			return graphQL.execute(input).toSpecification();
		}
	}
}
