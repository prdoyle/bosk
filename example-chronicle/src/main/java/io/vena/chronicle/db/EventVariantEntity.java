package io.vena.chronicle.db;

/**
 * Being a variant record, the intent is that exactly one of the members should be non-null.
 */
public record EventVariantEntity(
	StartRecordEntity start,
	StopRecordEntity stop
){ }
