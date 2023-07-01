package io.vena.chronicle.dto;

import java.util.Map;

public record StopRecordDTO(
	String spanID,
	Map<String, Object> attributes
){ }
