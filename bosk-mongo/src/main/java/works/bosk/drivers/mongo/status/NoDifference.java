package works.bosk.drivers.mongo.status;

public record NoDifference() implements Difference {
	@Override
	public NoDifference withPrefix(String prefix) {
		return this;
	}
}
