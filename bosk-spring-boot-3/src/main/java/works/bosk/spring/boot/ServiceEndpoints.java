package works.bosk.spring.boot;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import works.bosk.Bosk;
import works.bosk.Entity;
import works.bosk.EnumerableByIdentifier;
import works.bosk.Identifier;
import works.bosk.Path;
import works.bosk.Reference;
import works.bosk.SerializationPlugin;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.exceptions.NonexistentReferenceException;
import works.bosk.jackson.JacksonPlugin;

@RestController
public class ServiceEndpoints {
	private final Bosk<?> bosk;
	private final ObjectMapper mapper;
	private final JacksonPlugin plugin;

	public ServiceEndpoints(
		Bosk<?> bosk,
		ObjectMapper mapper,
		JacksonPlugin plugin,
		@Value("${bosk.web.service-path}") String contextPath
	) {
		this.bosk = bosk;
		this.mapper = mapper;
		this.plugin = plugin;
	}

	@GetMapping(produces = MediaType.APPLICATION_JSON_VALUE, path = {
		"${bosk.web.service-path}",
		"${bosk.web.service-path}/",
		"${bosk.web.service-path}/{path:.+}"
	})
	Object getAny(HttpServletRequest req, HttpServletResponse rsp, @PathVariable(value="path", required = false) String path) {
		LOGGER.debug("{} {}", req.getMethod(), req.getRequestURI());
		rsp.setContentType(MediaType.APPLICATION_JSON_VALUE);
		Reference<?> ref = referenceForPath(path);
		try {
			return ref.value();
		} catch (NonexistentReferenceException e) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Object does not exist: " + ref, e);
		}
	}

	@PutMapping(path = {
		"${bosk.web.service-path}",
		"${bosk.web.service-path}/",
		"${bosk.web.service-path}/{path:.+}"
	})
	<T> void putAny(HttpServletRequest req, HttpServletResponse rsp, @PathVariable("path") String path) throws IOException, InvalidTypeException {
		LOGGER.debug("{} {}", req.getMethod(), req.getRequestURI());
		@SuppressWarnings("unchecked")
		Reference<T> ref = (Reference<T>) referenceForPath(path);
		T newValue;
		try (@SuppressWarnings("unused") SerializationPlugin.DeserializationScope scope = plugin.newDeserializationScope(ref)) {
			newValue = mapper
				.readerFor(mapper.constructType(ref.targetType()))
				.readValue(req.getReader());
		}
		checkForMismatchedID(ref, newValue);
		discriminatePreconditionCases(req, new PreconditionDiscriminator() {
			@Override
			public void ifUnconditional() {
				bosk.driver().submitReplacement(ref, newValue);
			}

			@Override
			public void ifMustMatch(Identifier expectedRevision) {
				bosk.driver().submitConditionalReplacement(
					ref, newValue,
					revisionRef(ref), expectedRevision);
			}

			@Override
			public void ifMustNotExist() {
				bosk.driver().submitInitialization(ref, newValue);
			}
		});
		rsp.setStatus(HttpStatus.ACCEPTED.value());
	}

	@DeleteMapping(path = {
		"${bosk.web.service-path}",
		"${bosk.web.service-path}/",
		"${bosk.web.service-path}/{path:.+}"
	})
	void deleteAny(HttpServletRequest req, HttpServletResponse rsp, @PathVariable("path") String path) {
		LOGGER.debug("{} {}", req.getMethod(), req.getRequestURI());
		Reference<?> ref = referenceForPath(path);
		discriminatePreconditionCases(req, new PreconditionDiscriminator() {
			@Override
			public void ifUnconditional() {
				bosk.driver().submitDeletion(ref);
			}

			@Override
			public void ifMustMatch(Identifier expectedRevision) {
				bosk.driver().submitConditionalDeletion(
					ref, revisionRef(ref), expectedRevision);
			}

			@Override
			public void ifMustNotExist() {
				// Request to delete a nonexistent object: nothing to do
			}
		});
		rsp.setStatus(HttpStatus.ACCEPTED.value());
	}

	private Reference<?> referenceForPath(String path) {
		if (path == null) {
			return bosk.rootReference();
		}
		try {
			return bosk.rootReference().then(Object.class, Path.parse("/" + path));
		} catch (InvalidTypeException e) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Invalid path: " + path, e);
		}
	}

	private Reference<Identifier> revisionRef(Reference<?> ref) {
		try {
			return ref.then(Identifier.class, "revision");
		} catch (InvalidTypeException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Preconditions not supported for object with no suitable revision field: " + ref, e);
		}
	}

	private interface PreconditionDiscriminator {
		void ifUnconditional();

		void ifMustMatch(Identifier expectedRevision);

		void ifMustNotExist();
	}

	/**
	 * ETags are a little fiddly to decode. This logic handles the various cases and error conditions,
	 * and then calls the given <code>discriminator</code> to perform the desired action.
	 */
	static void discriminatePreconditionCases(HttpServletRequest req, PreconditionDiscriminator discriminator) {
		String ifMatch = req.getHeader("If-Match");
		String ifNoneMatch = req.getHeader("If-None-Match");
		LOGGER.debug("| If-Match: {} -- If-None-Match: {}", ifMatch, ifNoneMatch);
		if (ifMatch == null) {
			if (ifNoneMatch == null) {
				LOGGER.trace("| Unconditional");
				discriminator.ifUnconditional();
			} else if ("*".equals(ifNoneMatch)) {
				LOGGER.trace("| MustNotExist");
				discriminator.ifMustNotExist();
			} else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "If-None-Match header, if supplied, must be \"*\"");
			}
		} else if (ifNoneMatch == null) {
			Identifier expectedRevision = Identifier.from(etagStringValue(ifMatch));
			LOGGER.trace("| MustMatch({})", expectedRevision);
			discriminator.ifMustMatch(expectedRevision);
		} else {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cannot supply both If-Match and If-None-Match");
		}
	}

	private static String etagStringValue(String etagString) {
		if (etagString.length() < 3 || etagString.charAt(0) != '"' || etagString.charAt(etagString.length() - 1) != '"') {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ETag string must be a non-empty string surrounded by quotes: " + etagString);
		}
		String value = etagString.substring(1, etagString.length() - 1);
		// We permit only the ASCII subset of https://datatracker.ietf.org/doc/html/rfc7232#section-2.3
		for (int i = 0; i < value.length(); i++) {
			int ch = value.codePointAt(i);
			if (ch == '"') {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only a single ETag string is supported: " + etagString);
			} else if (ch == 0x21 || (0x23 <= ch && ch <= 0x7E)) { // Note: 0x22 is the quote character
				// all is well
			} else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ETag string contains an unsupported character at position " + i + ", code point " + ch + ": " + etagString);
			}
		}
		return value;
	}

	private void checkForMismatchedID(Reference<?> ref, Object newValue) throws InvalidTypeException {
		if (newValue instanceof Entity e && !ref.path().isEmpty()) {
			Reference<?> enclosingRef = ref.enclosingReference(Object.class);
			if (EnumerableByIdentifier.class.isAssignableFrom(enclosingRef.targetClass())) {
				Identifier pathID = Identifier.from(ref.path().lastSegment());
				Identifier entityID = e.id();
				if (!pathID.equals(entityID)) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ref.getClass().getSimpleName() + " ID \"" + entityID + "\" does not match path ID \"" + pathID + "\"");
				}
			}
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceEndpoints.class);
}
