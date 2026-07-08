package works.bosk.drivers.mongo.internal;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorRecordingChangeListener extends ForwardingChangeListener {
	final ErrorRecorder errorRecorder;

	public static class ErrorRecorder {
		public int failureCount = 0;
		public final List<Throwable> disconnections = new ArrayList<>();

		public void assertAllClear(String description) {
			assertTrue(failureCount == 0 && disconnections.isEmpty(),
				"Expected no errors " + description + "; found " + this);
		}

		@Override
		public String toString() {
			return "ErrorRecorder{" +
				"failureCount=" + failureCount +
				", disconnections=" + disconnections +
				'}';
		}
	}

	public ErrorRecordingChangeListener(ErrorRecorder errorRecorder, ChangeListener downstream) {
		super(downstream);
		this.errorRecorder = errorRecorder;
	}

	@Override
	public void onConnectionFailed(Exception cause) throws DownstreamInitialStateException {
		errorRecorder.failureCount++;
		super.onConnectionFailed(cause);
	}

	@Override
	public void onDisconnect(Throwable e) {
		errorRecorder.disconnections.add(e);
		super.onDisconnect(e);
	}
}
