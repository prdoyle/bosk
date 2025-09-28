package works.bosk.json.codec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static works.bosk.json.codec.ParserSessionImpl.fast_isInsignificant;
import static works.bosk.json.codec.ParserSessionImpl.isInsignificant;

class ParserSessionImplTest {

	@Test
	void testIsInsignificant() {
		int[] insignificant = new int[]{0x20, 0x0A, 0x0D, 0x09, ',', ':'};
		for (int i: insignificant) {
			for (int delta: new int[]{1, -1, 64, -64}) {
				assertEquals(isInsignificant(i+delta), fast_isInsignificant(i+delta), i + "+" + delta);
			}
		}
	}

}
