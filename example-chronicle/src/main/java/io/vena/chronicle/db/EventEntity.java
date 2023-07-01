package io.vena.chronicle.db;

public record EventEntity(
	MetadataEntity meta,
	EventVariantEntity data
) { }
