module works.bosk.core {
	requires org.jetbrains.annotations;
	requires org.objectweb.asm.util;
	requires org.pcollections;
	requires org.slf4j;
	requires works.bosk.annotations;
	requires static lombok;

	exports works.bosk;
	exports works.bosk.drivers;
	exports works.bosk.exceptions;
	exports works.bosk.util;
	exports works.bosk.bytecode to works.bosk.jackson;
	exports works.bosk.logging to works.bosk.mongo, works.bosk.logback;
}
