package org.vena.bosk.drivers;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.lang.reflect.Type;
import org.vena.bosk.BoskDriver;
import org.vena.bosk.Entity;
import org.vena.bosk.Identifier;
import org.vena.bosk.Reference;
import org.vena.bosk.exceptions.InvalidTypeException;

import static io.opentelemetry.api.trace.StatusCode.OK;
import static java.lang.String.format;
import static org.vena.bosk.ReferenceUtils.rawClass;

public class TracingDriver<R extends Entity> implements BoskDriver<R> {
	private final Tracer tracer;
	private final Attributes customAttributes;
	private final BoskDriver<R> downstream;

	/**
	 * @param tracerProvider from which this driver will {@link TracerProvider#get get} a {@link Tracer}
	 * @param customAttributes collection of attributes to add to every span, in addition to
	 *                         the build-in ones that {@link TracingDriver} adds;
	 *                         use {@link Attributes#empty()} if none
	 * @param downstream the {@link BoskDriver} to which all calls should be forwarded
	 */
	public TracingDriver(TracerProvider tracerProvider, Attributes customAttributes, BoskDriver<R> downstream) {
		customAttributes.forEach((k,v) -> {
			if (k.getKey().startsWith("bosk.")) {
				throw new IllegalArgumentException(format("TracingDriver: Custom attributes cannot start with \"bosk.\" prefix: {%s:%s}", k, v));
			}
		});

		this.tracer = tracerProvider.get(getClass().getSimpleName());
		this.downstream = downstream;
		this.customAttributes = customAttributes;
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		Span span = spanBuilder("initialRoot")
			.setAttribute(ATTR_ROOT_CLASS, rawClass(rootType).getName())
			.setAttribute(ATTR_ROOT_TYPE, rootType.getTypeName())
			.startSpan();
		try (Scope ignored = span.makeCurrent()) {
			R result = downstream.initialRoot(rootType);
			span.setStatus(OK);
			return result;
		} catch (Throwable t) {
			span.recordException(t);
			throw t;
		} finally {
			span.end();
		}
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		Span span = spanBuilder("submitReplacement")
			.setAttribute(ATTR_TARGET_PATH, target.toString())
			.setAttribute(ATTR_TARGET_CLASS, target.targetClass().getName())
			.setAttribute(ATTR_TARGET_TYPE, target.targetType().getTypeName())
			.startSpan();
		try (Scope ignored = span.makeCurrent()) {
			downstream.submitReplacement(target, newValue);
			span.setStatus(OK);
		} catch (Throwable t) {
			span.recordException(t);
			throw t;
		} finally {
			span.end();
		}
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		Span span = spanBuilder("submitConditionalReplacement")
			.setAttribute(ATTR_TARGET_PATH, target.pathString())
			.setAttribute(ATTR_TARGET_CLASS, target.targetClass().getName())
			.setAttribute(ATTR_TARGET_TYPE, target.targetType().getTypeName())
			.setAttribute(ATTR_PRECONDITION_PATH, precondition.pathString())
			.setAttribute(ATTR_PRECONDITION_REQ, requiredValue.toString())
			.startSpan();
		try (Scope ignored = span.makeCurrent()) {
			downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
			span.setStatus(OK);
		} catch (Throwable t) {
			span.recordException(t);
			throw t;
		} finally {
			span.end();
		}
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		Span span = spanBuilder("submitInitialization")
			.setAttribute(ATTR_TARGET_PATH, target.pathString())
			.setAttribute(ATTR_TARGET_CLASS, target.targetClass().getName())
			.setAttribute(ATTR_TARGET_TYPE, target.targetType().getTypeName())
			.startSpan();
		try (Scope ignored = span.makeCurrent()) {
			downstream.submitInitialization(target, newValue);
			span.setStatus(OK);
		} catch (Throwable t) {
			span.recordException(t);
			throw t;
		} finally {
			span.end();
		}
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		Span span = spanBuilder("submitDeletion")
			.setAttribute(ATTR_TARGET_PATH, target.toString())
			.setAttribute(ATTR_TARGET_CLASS, target.targetClass().getName())
			.setAttribute(ATTR_TARGET_TYPE, target.targetType().getTypeName())
			.startSpan();
		try (Scope ignored = span.makeCurrent()) {
			downstream.submitDeletion(target);
			span.setStatus(OK);
		} catch (Throwable t) {
			span.recordException(t);
			throw t;
		} finally {
			span.end();
		}
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		Span span = spanBuilder("submitConditionalDeletion")
			.setAttribute(ATTR_TARGET_PATH, target.pathString())
			.setAttribute(ATTR_TARGET_CLASS, target.targetClass().getName())
			.setAttribute(ATTR_TARGET_TYPE, target.targetType().getTypeName())
			.setAttribute(ATTR_PRECONDITION_PATH, precondition.pathString())
			.setAttribute(ATTR_PRECONDITION_REQ, requiredValue.toString())
			.startSpan();
		try (Scope ignored = span.makeCurrent()) {
			downstream.submitConditionalDeletion(target, precondition, requiredValue);
			span.setStatus(OK);
		} catch (Throwable t) {
			span.recordException(t);
			throw t;
		} finally {
			span.end();
		}
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		Span span = spanBuilder("flush")
			.startSpan();
		try (Scope ignored = span.makeCurrent()) {
			downstream.flush();
			span.setStatus(OK);
		} catch (Throwable t) {
			span.recordException(t);
			throw t;
		} finally {
			span.end();
		}
	}

	private SpanBuilder spanBuilder(String operationName) {
		SpanBuilder result = tracer.spanBuilder("BoskDriver." + operationName)
//			.setAllAttributes(customAttributes) // Not available until API version 1.2.0
			.setAttribute(ATTR_OPERATION, operationName);
		customAttributes.forEach((k,v)-> addAttributeToSpanBuilder(result, k, v));
		return result;
	}

	@SuppressWarnings("unchecked")
	private void addAttributeToSpanBuilder(SpanBuilder result, AttributeKey<?> key, Object value) {
		@SuppressWarnings("rawtypes")
		AttributeKey erasedKey = key;
		result.setAttribute(erasedKey, value);
	}

	static final AttributeKey<String> ATTR_OPERATION = AttributeKey.stringKey(
		"bosk.driver.operation");
	static final AttributeKey<String> ATTR_ROOT_CLASS = AttributeKey.stringKey(
		"bosk.driver.root.class");
	static final AttributeKey<String> ATTR_ROOT_TYPE = AttributeKey.stringKey(
		"bosk.driver.root.type");
	static final AttributeKey<String> ATTR_TARGET_PATH = AttributeKey.stringKey(
		"bosk.driver.target.path");
	static final AttributeKey<String> ATTR_TARGET_CLASS = AttributeKey.stringKey(
		"bosk.driver.target.class");
	static final AttributeKey<String> ATTR_TARGET_TYPE = AttributeKey.stringKey(
		"bosk.driver.target.type");
	static final AttributeKey<String> ATTR_PRECONDITION_PATH = AttributeKey.stringKey(
		"bosk.driver.precondition.path");
	static final AttributeKey<String> ATTR_PRECONDITION_REQ = AttributeKey.stringKey(
		"bosk.driver.precondition.required");
}
