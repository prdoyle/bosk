package works.bosk.hello;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import works.bosk.Catalog;
import works.bosk.Identifier;
import works.bosk.hello.state.BoskState;
import works.bosk.hello.state.Target;

import static org.springframework.http.MediaType.APPLICATION_FORM_URLENCODED;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = HelloApplication.class)
@AutoConfigureMockMvc
public class HelloServiceEndpointsTest {
	static final Target INITIAL_TARGET = new Target(Identifier.from("world"));
	static final BoskState INITIAL_STATE = new BoskState(Catalog.of(INITIAL_TARGET));

	@Autowired
	MockMvc mvc;

	@Autowired
	ObjectMapper mapper;

	@Autowired
	HelloBosk bosk;

	@BeforeEach
	void setupBosk() throws IOException, InterruptedException {
		bosk.driver().submitReplacement(bosk.rootReference(), INITIAL_STATE);
		bosk.driver().flush();
	}

	@ParameterizedTest
	@ValueSource(strings = {"/bosk","/bosk/"})
	void get_root_works(String uri) throws Exception {
		assertGetReturns(INITIAL_STATE, uri);
		assertHello("world");
	}

	@ParameterizedTest
	@ValueSource(strings = {"/bosk","/bosk/"})
	void put_root_works(String uri) throws Exception {
		var newValue = new BoskState(
			INITIAL_STATE.targets().with(new Target(Identifier.from("everybody")))
		);
		mvc.perform(put(uri)
				.contentType(APPLICATION_JSON)
				.content(mapper.writeValueAsString(newValue)))
			.andExpect(status().isAccepted());
		assertGetReturns(newValue, uri);
		assertHello("world", "everybody");
	}

	@ParameterizedTest
	@ValueSource(strings = {"/bosk","/bosk/"})
	void delete_root_reportsError(String uri) throws Exception {
		mvc.perform(delete(uri))
			.andExpect(status().isBadRequest());
		assertHello("world");
	}

	@Test
	void get_targets_works() throws Exception {
		assertGetReturns(INITIAL_STATE.targets(), "/bosk/targets");
	}

	@Test
	void put_targets_works() throws Exception {
		var newTargets = INITIAL_STATE.targets().with(new Target(Identifier.from("new target")));
		mvc.perform(put("/bosk/targets")
				.contentType(APPLICATION_JSON)
				.content(mapper.writeValueAsString(newTargets)))
			.andExpect(status().isAccepted());
		assertGetReturns(newTargets, "/bosk/targets");
		assertHello("world", "new target");
	}

	@Test
	void delete_targets_reportsError() throws Exception {
		mvc.perform(delete("/bosk/targets"))
			.andExpect(status().isBadRequest());
		assertHello("world");
	}

	@Test
	void get_existingTarget_works() throws Exception {
		assertGetReturns(INITIAL_TARGET, "/bosk/targets/" + INITIAL_TARGET.id());
	}

	@Test
	void get_nonexistentTarget_reportsError() throws Exception {
		mvc.perform(get("/bosk/targets/nonexistent"))
			.andExpect(status().isNotFound());
	}

	/**
	 * It's hard to tell whether this is doing anything, because
	 * {@link Target} has no other fields besides its <code>id</code>.
	 */
	@Test
	void put_existingTarget_works() throws Exception {
		String uri = "/bosk/targets/" + INITIAL_TARGET.id();
		mvc.perform(put(uri)
				.contentType(APPLICATION_JSON)
				.content(mapper.writeValueAsString(INITIAL_TARGET)))
			.andExpect(status().isAccepted());
		assertGetReturns(INITIAL_TARGET, uri);
		assertHello("world");
	}

	@Test
	void put_newTarget_works() throws Exception {
		var newTarget = new Target(Identifier.from("new target"));
		String uri = "/bosk/targets/" + newTarget.id();
		mvc.perform(put(uri)
				.contentType(APPLICATION_JSON)
				.content(mapper.writeValueAsString(newTarget)))
			.andExpect(status().isAccepted());
		assertGetReturns(newTarget, uri);
		assertHello("world", "new target");
	}

	@Test
	void put_wrongContentType_reportsError() throws Exception {
		mvc.perform(put("/bosk/targets/" + INITIAL_TARGET.id())
				.contentType(APPLICATION_FORM_URLENCODED)
				.content(mapper.writeValueAsString(INITIAL_TARGET)))
			.andExpect(status().isUnsupportedMediaType());
	}

	@Test
	void delete_existingTarget_works() throws Exception {
		mvc.perform(delete("/bosk/targets/" + INITIAL_TARGET.id()))
			.andExpect(status().isAccepted());
		mvc.perform(get("/bosk/targets/" + INITIAL_TARGET.id()))
			.andExpect(status().isNotFound());
		assertHello();
	}

	private void assertGetReturns(Object object, String uri) throws Exception {
		String expected = mapper.writeValueAsString(object);
		mvc.perform(get(uri))
			.andExpect(status().isOk())
			.andExpect(content().json(expected));
	}

	private void assertHello(String... targets) throws Exception {
		record Response(List<String> greetings){}
		var expected = new Response(Stream.of(targets)
			.map(t -> "Hello, " + t + "!").toList());
		mvc.perform(get("/api/hello"))
			.andExpect(status().isOk())
			.andExpect(content().contentType(APPLICATION_JSON))
			.andExpect(content().json(mapper.writeValueAsString(expected)));
	}
}
