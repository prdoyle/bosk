package works.bosk.drivers.mongo.status;

sealed public interface SomeDifference extends Difference permits
	MultipleDifferences,
	UnexpectedNode,
	NodeMissing,
	PrimitiveDifference
{
	String bsonPath();
}
