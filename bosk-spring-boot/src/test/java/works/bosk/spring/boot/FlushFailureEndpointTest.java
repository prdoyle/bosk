package works.bosk.spring.boot;

import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.security.autoconfigure.UserDetailsServiceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import works.bosk.Bosk;
import works.bosk.BoskConfig;
import works.bosk.StateTreeNode;
import works.bosk.drivers.ForwardingDriver;
import works.bosk.exceptions.RetryableFlushException;
import works.bosk.jackson.JacksonSerializer;

import static org.springframework.http.HttpHeaders.CACHE_CONTROL;
import static org.springframework.http.HttpHeaders.RETRY_AFTER;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static works.bosk.testing.BoskTestUtils.boskName;

@SpringBootTest(
	classes = FlushFailureEndpointTest.TestConfig.class,
	properties = {
		"bosk.web.maintenance.access=AUTHENTICATED",
		"bosk.web.maintenance.path=/bosk"
	})
@AutoConfigureMockMvc
class FlushFailureEndpointTest {
	// Arbitrary, and distinct from the 100 s default, so the test proves the
	// filter uses the exception's hint.
	private static final Duration RETRY_AFTER_HINT = Duration.ofSeconds(42);

	@Autowired
	MockMvc mockMvc;

	public record State() implements StateTreeNode {}

	@Configuration
	@EnableAutoConfiguration(exclude = UserDetailsServiceAutoConfiguration.class)
	static class TestConfig {
		@Bean
		Bosk<State> bosk() {
			return new Bosk<>(
				boskName(),
				State.class,
				_ -> new State(),
				BoskConfig.<State>builder()
					.driverFactory((_, downstream) -> new ForwardingDriver(downstream) {
						@Override
						public void flush() throws IOException {
							throw new RetryableFlushException("Disconnected", RETRY_AFTER_HINT);
						}
					})
					.build()
			);
		}

		@Bean
		JacksonSerializer jacksonSerializer() {
			return new JacksonSerializer();
		}

		@Bean
		ObjectMapper objectMapper(Bosk<State> bosk, JacksonSerializer jacksonSerializer) {
			return JsonMapper.builder()
				.addModule(jacksonSerializer.moduleFor(bosk))
				.build();
		}
	}

	@Test
	void noCache_flushFailure_returns503() throws Exception {
		mockMvc.perform(get("/bosk")
				.header(CACHE_CONTROL, "no-cache")
				.with(user("tester")))
			.andExpect(status().isServiceUnavailable())
			.andExpect(header().string(RETRY_AFTER, Long.toString(RETRY_AFTER_HINT.toSeconds())))
			.andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
			.andExpect(content().json("""
				{"error":"bosk_flush_failed","message":"The server could not confirm that the state is up to date.","retryAfterSeconds":%d}
				""".formatted(RETRY_AFTER_HINT.toSeconds())));
	}
}
