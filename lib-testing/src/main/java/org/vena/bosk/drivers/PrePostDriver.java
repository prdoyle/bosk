package org.vena.bosk.drivers;

import java.io.IOException;
import java.lang.reflect.Type;
import lombok.RequiredArgsConstructor;
import org.vena.bosk.BoskDriver;
import org.vena.bosk.Entity;
import org.vena.bosk.Identifier;
import org.vena.bosk.Reference;
import org.vena.bosk.exceptions.InvalidTypeException;

@RequiredArgsConstructor
public class PrePostDriver<R extends Entity> implements BoskDriver<R> {
	private final Runnable pre, post;
	private final BoskDriver<R> downstream;

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		pre.run();
		try {
			return downstream.initialRoot(rootType);
		} finally {
			post.run();
		}
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		pre.run();
		try {
			downstream.submitReplacement(target, newValue);
		} finally {
			post.run();
		}
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		pre.run();
		try {
			downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
		} finally {
			post.run();
		}
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		pre.run();
		try {
			downstream.submitInitialization(target, newValue);
		} finally {
			post.run();
		}
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		pre.run();
		try {
			downstream.submitDeletion(target);
		} finally {
			post.run();
		}
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		pre.run();
		try {
			downstream.submitConditionalDeletion(target, precondition, requiredValue);
		} finally {
			post.run();
		}
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		pre.run();
		try {
			downstream.flush();
		} finally {
			post.run();
		}
	}
}
