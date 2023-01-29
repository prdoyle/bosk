package io.vena.bosk.spring;

import io.vena.bosk.Bosk;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

/**
 * For "GET-like requests", automatically runs the request with a {@link Bosk.ReadContext ReadContext}.
 *
 * <p>
 * It's unusual for a non-GET request to need a ReadContext; almost an anti-pattern.
 * If they need one, they can open one.
 */
@RequiredArgsConstructor
public class ReadContextFilter implements Filter {
	private final Bosk<?> bosk;

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		if (needsReadContext(request)) {
			try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = bosk.readContext()) {
				chain.doFilter(request, response);
			}
		} else {
			chain.doFilter(request, response);
		}
	}

	public boolean needsReadContext(ServletRequest request) {
		if (!(request instanceof HttpServletRequest)) {
			return false;
		}
		String method = ((HttpServletRequest)request).getMethod();
		return method.equalsIgnoreCase("GET")
			|| method.equalsIgnoreCase("HEAD")
			|| method.equalsIgnoreCase("POST");
	}

}
