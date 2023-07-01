package io.vena.chronicle.db;

public record SpanDataEntity(
	String name,
	String trace,
	String parent
) { }
