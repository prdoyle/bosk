package io.vena.bosk.drivers.mongo;

import com.mongodb.client.ClientSession;
import io.vena.bosk.Bosk;

/**
 * Evolution note: we're going to want to get organized in how we generate MDC.
 * For now, it's all in bosk-mongo, so we can put these keys here.
 */
final class MdcKeys {
	/**
	 * The value of {@link Bosk#name()}.
	 */
	static final String BOSK_NAME = "bosk.name";

	/**
	 * The value of {@link Bosk#instanceID()}.
	 */
	static final String BOSK_INSTANCE_ID = "bosk.instanceID";

	/**
	 * A unique string generated for each MongoDB change event received by a particular bosk.
	 */
	static final String EVENT = "bosk.MongoDriver.event";

	/**
	 * A unique string generated for each MongoDB {@link ClientSession}.
	 * Technically, not every session is a transaction, but we do use them for
	 * transactions, and this name seemed to convey the intent better than "session".
	 */
	 static final String TRANSACTION = "bosk.MongoDriver.transaction";
}
