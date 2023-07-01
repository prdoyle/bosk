package io.vena.chronicle.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public record SpanDataDTO(
	String name,
	String trace,
	@JsonInclude(NON_NULL) String parent
) { }
