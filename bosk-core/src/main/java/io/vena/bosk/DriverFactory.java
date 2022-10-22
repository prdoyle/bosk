package io.vena.bosk;

public interface DriverFactory<R extends Entity> {
	BoskDriver<R> build(ReferenceFactory<R> refs, BoskDriver<R> downstream);
}
