package works.bosk.drivers.mongo.status;

public record StateStatus(
	Long revision,
	Long sizeInBytes,
	Difference difference
) { }
