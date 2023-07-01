package io.vena.chronicle.dto;

import java.time.Instant;

public record EventDTO (
	MetadataDTO meta,
	EventVariantDTO data
){
	public EventDTO ingestedNow() {
		return new EventDTO(
			new MetadataDTO(Instant.now().toEpochMilli()),
			data
		);
	}
}
