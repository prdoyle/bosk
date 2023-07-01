package io.vena.chronicle.dto;

import java.util.Map;

public record StartRecordDTO(
	String spanID,
	SpanDataDTO span,
	Map<String, Object> context
){ }
