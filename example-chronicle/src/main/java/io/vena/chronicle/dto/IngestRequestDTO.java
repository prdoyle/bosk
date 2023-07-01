package io.vena.chronicle.dto;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import java.util.List;

@JsonAutoDetect
public record IngestRequestDTO(
	Long ingestionTime,
	List<EventDTO> items
) { }
