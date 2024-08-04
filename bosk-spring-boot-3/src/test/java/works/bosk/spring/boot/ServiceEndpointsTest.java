package works.bosk.spring.boot;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import works.bosk.Bosk;
import works.bosk.StateTreeNode;
import works.bosk.drivers.BufferingDriver;
import works.bosk.jackson.JacksonPlugin;
import works.bosk.spring.boot.ServiceEndpointsTest.BoskState;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static works.bosk.spring.boot.ServiceEndpointsTest.GREETING_VALUE;
import static works.bosk.spring.boot.TestBosk.INITIAL_STATE;

/**
 * Only a partial test because I can't seem to coax Jackson to work.
 * <p>
 * More complete testing can be found in <code>HelloServiceEndpointsTest</code>.
 */
@SpringBootTest(classes={Config.class, ServiceEndpoints.class, ReadContextFilter.class})
@AutoConfigureMockMvc
@TestPropertySource(properties = {"bosk.web.service-path=/bosk"})
public class ServiceEndpointsTest {
	public record BoskState(Optional<String> greeting) implements StateTreeNode { }
	public static final String GREETING_PATH = "/bosk/greeting";
	public static final String GREETING_VALUE = "Hello, world!";

	@Autowired
	private MockMvc mvc;

	@Autowired
	private TestBosk bosk;

	@Autowired
	private ObjectMapper mapper;

	@BeforeEach
	void setUpBosk() throws IOException, InterruptedException {
		bosk.driver().submitReplacement(bosk.rootReference(), INITIAL_STATE);
		bosk.driver().flush();
	}

	@Test
	void get_works() throws Exception {
		mvc.perform(get(GREETING_PATH))
			.andExpect(status().isOk())
			.andExpect(content().string(GREETING_VALUE));
	}

	@Test
	void put_works() throws Exception {
		mvc.perform(put(GREETING_PATH).content("\"New greeting\""))
			.andExpect(status().isAccepted());
		bosk.driver().flush();
		mvc.perform(get(GREETING_PATH))
			.andExpect(status().isOk())
			.andExpect(content().string("New greeting"));
	}

	@Test
	void delete_works() throws Exception {
		mvc.perform(delete(GREETING_PATH))
			.andExpect(status().isAccepted());
		bosk.driver().flush();
		mvc.perform(get(GREETING_PATH))
			.andExpect(status().isNotFound());
	}

	@Disabled("Can't seem to get Jackson working. Without this, we can't test etags either.")
	@ParameterizedTest
	@ValueSource(strings = {"/bosk","/bosk/"})
	void get_root_works() throws Exception {
		String expected = mapper.writeValueAsString(INITIAL_STATE);
		mvc.perform(get("/bosk/"))
			.andExpect(status().isOk())
			.andExpect(content().json(expected));
	}

	@ParameterizedTest
	@ValueSource(strings = {"/bosk","/bosk/"})
	void delete_root_badRequest(String uri) throws Exception {
		mvc.perform(delete(uri))
			.andExpect(status().isBadRequest());
	}

	private void assertGetReturns(Object object, String uri) throws Exception {
		String expected = mapper.writeValueAsString(object);
		mvc.perform(get(uri))
			.andExpect(status().isOk())
			.andExpect(content().json(expected));
	}
}

class TestBosk extends Bosk<BoskState> {
	public static final BoskState INITIAL_STATE = new BoskState(Optional.of(GREETING_VALUE));

	public TestBosk() {
		super(
			"TestBosk",
			BoskState.class,
			INITIAL_STATE,
			// Let's not cheat and skip the flushes here. In the future, our ReadContextFilter
			// could support flush functionality, and we'll want to test that.
			// Better to make the existing tests correct so that they work under those new conditions.
			BufferingDriver.factory()
		);
	}
}

@Configuration
class Config {
	@Bean
	public TestBosk bosk() {
		return new TestBosk();
	}

	@Bean
	public JacksonPlugin jacksonPlugin() {
		return new JacksonPlugin();
	}

	@Bean
	ObjectMapper objectMapper(TestBosk bosk, JacksonPlugin jacksonPlugin) {
		return new ObjectMapper()
			.registerModule(jacksonPlugin.moduleFor(bosk))
			.enable(INDENT_OUTPUT);
	}

	@Bean
	public FilterRegistrationBean<ReadContextFilter> filter(TestBosk bosk) {
		FilterRegistrationBean<ReadContextFilter> registrationBean = new FilterRegistrationBean<>();
		registrationBean.setFilter(new ReadContextFilter(bosk));
		registrationBean.addUrlPatterns("/bosk*");
		return registrationBean;
	}

}
