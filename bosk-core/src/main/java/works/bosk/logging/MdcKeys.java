package works.bosk.logging;

/**
 * Keys to use for SLF4J's Mapped Diagnostic Context.
 * <p>
 * Evolution note: we're going to want to get organized in how we generate MDC.
 * For now, only bosk-mongo uses these.
 */
public final class MdcKeys {
	public static final String BOSK_NAME        = "bosk.name";
	public static final String BOSK_INSTANCE_ID = "bosk.instanceID";
	public static final String EVENT            = "bosk.MongoDriver.event";
	public static final String TRANSACTION      = "bosk.MongoDriver.transaction";
}
