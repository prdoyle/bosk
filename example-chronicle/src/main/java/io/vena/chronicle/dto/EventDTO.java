package io.vena.chronicle.dto;

public record EventDTO (
	MetadataDTO meta,
	EventVariantDTO data
){
	public EventDTO withMeta(MetadataDTO meta) {
		return new EventDTO(meta, this.data);
	}
}
