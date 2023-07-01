package io.vena.chronicle.dto;

import java.util.List;

public record EventListDTO(
	List<EventDTO> items
) { }
