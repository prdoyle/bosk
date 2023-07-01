package io.vena.chronicle.db;

import java.util.Map;

/**
 * Records the end of a span.
 * Includes information collected while the span executed.
 *
 * <p>
 * Attribute names should follow OpenTelemetry semantic conventions.
 * Ideally, {@link #attributes()} names are disjoint with those of {@link StartRecordEntity#context()};
 * if some names are present in both, ideally the values would be identical.
 * During processing, the two can be combined into a single map, with conflicts
 * resolved by preferring the value from {@link #attributes()}.
 *
 * @param attributes is conceptually equivalent to {@link StartRecordEntity#context()}
 */
public record StopRecordEntity(
	String spanID,
	Map<String, Object> attributes
){ }
