package works.bosk;

class TunneledCheckedException extends RuntimeException {
	public TunneledCheckedException(Throwable cause) {
		super(cause);
	}

	@Override
	public Throwable getCause() {
		return super.getCause();
	}
}
