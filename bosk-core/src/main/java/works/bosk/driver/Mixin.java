package works.bosk.driver;

import java.util.List;

public interface Mixin {
	List<DriverSpec> appliedTo(List<DriverSpec> newSpecs);
}
