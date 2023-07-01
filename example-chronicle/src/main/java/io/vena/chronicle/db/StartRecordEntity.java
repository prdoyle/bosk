package io.vena.chronicle.db;

import java.util.Map;

/**
 * Records the start of a span.
 * Includes information known at the time the span begins.
 *
 * @param context is conceptually equivalent to {@link StopRecordEntity#attributes()}
 *                except that {@link #context()} is available at the time the span starts.
 *                The difference only matters for spans that have not yet finished.
 *                When ingesting telemetry that reports only completed spans,
 *                this field can be empty, and context info can be stored in the
 *                {@link StopRecordEntity} instead.
 */
public record StartRecordEntity(
	String spanID,
	SpanDataEntity span,
	Map<String, Object> context
){ }
