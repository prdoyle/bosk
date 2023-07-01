package io.vena.chronicle.db;

/**
 * Data supplied by Chronicle.
 * Ignored for ingested events.
 */
public record MetadataEntity (
	Long ingestedTime,
	SenderEntity sender
) { }
