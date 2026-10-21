package works.bosk.spring.boot;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.filter.OncePerRequestFilter;
import works.bosk.Bosk;
import works.bosk.exceptions.NoReadSessionException;
import works.bosk.exceptions.RetryableFlushException;

import static org.springframework.http.HttpHeaders.CACHE_CONTROL;
import static org.springframework.http.HttpHeaders.RETRY_AFTER;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@Component
@ControllerAdvice
@RequiredArgsConstructor
public class ReadSessionFilter extends OncePerRequestFilter {
	private final Bosk<?> bosk;

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
		if ("no-cache".equalsIgnoreCase(request.getHeader(CACHE_CONTROL))) {
			// Allow the client to specify that they want their read session to have the latest state.
			// We do this even for requests that don't automatically open a read session because
			// they might later manually open one.
			try {
				bosk.driver().flush();
			} catch (InterruptedException e) {
				// Assume the user wanted to interrupt the entire request,
				// not just the flush operation.
				Thread.currentThread().interrupt();
				throw new ServletException(e);
			} catch (RetryableFlushException e) {
				writeFlushFailure(response, e);
				return;
			}
		}
		if (automaticallyOpenReadSession(request)) {
			try (var _ = bosk.readSession()) {
				filterChain.doFilter(request, response);
			}
		} else {
			filterChain.doFilter(request, response);
		}
	}

	/**
	 * The "safe" HTTP methods won't change the server state,
	 * so there's no reason not to open a read session.
	 */
	private boolean automaticallyOpenReadSession(HttpServletRequest request) {
		return switch (request.getMethod()) {
			case "GET", "HEAD", "OPTIONS" -> true;
			default -> false;
		};
	}

	private static void writeFlushFailure(HttpServletResponse response, RetryableFlushException e) throws IOException {
		long retryAfterSeconds = retryAfterSeconds(e.retryAfter());
		response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
		response.setHeader(RETRY_AFTER, Long.toString(retryAfterSeconds));
		response.setContentType(APPLICATION_JSON_VALUE);
		response.setCharacterEncoding(StandardCharsets.UTF_8.name());
		response.getWriter().write(
			"{\"error\":\"bosk_flush_failed\","
				+ "\"message\":\"The server could not confirm that the state is up to date.\","
				+ "\"retryAfterSeconds\":" + retryAfterSeconds + "}");
		LOGGER.debug("Flush failed; returning 503 with retryAfterSeconds={}", retryAfterSeconds, e);
	}

	private static long retryAfterSeconds(Duration retryAfter) {
		return Math.max(1, (retryAfter.toMillis() + 999) / 1000);
	}

	@ExceptionHandler(NoReadSessionException.class)
	void handleException(HttpServletRequest request, HttpServletResponse response) throws IOException {
		LOGGER.error("Bosk read session was not opened automatically; the request handler method should open one by calling Bosk.readSession(). " +
			"Request: {} {}", request.getMethod(), request.getRequestURI());
		response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ReadSessionFilter.class);
}
