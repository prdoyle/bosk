package io.vena.chronicle.db;

import java.util.Map;

public record StopRecordEntity(
	String spanID,
	Map<String, Object> attributes
){ }
