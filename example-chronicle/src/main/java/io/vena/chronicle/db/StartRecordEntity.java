package io.vena.chronicle.db;

import java.util.Map;

public record StartRecordEntity(
	String spanID,
	SpanDataEntity span,
	Map<String, Object> context
){ }
