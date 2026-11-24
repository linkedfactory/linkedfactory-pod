package io.github.linkedfactory.core.kvin.leveldb;

import io.github.linkedfactory.core.kvin.KvinException;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KvinLevelDbAwaitTest {
	@After
	public void clearInterrupt() {
		Thread.interrupted();
	}

	@Test
	public void testCheckedFailureIsWrapped() {
		IOException cause = new IOException("disk full");
		KvinException e = assertThrows(KvinException.class,
				() -> KvinLevelDb.await(CompletableFuture.failedFuture(cause)));
		assertSame(cause, e.getCause());
	}

	@Test
	public void testUncheckedFailureIsRethrown() {
		IllegalStateException cause = new IllegalStateException("closed");
		IllegalStateException e = assertThrows(IllegalStateException.class,
				() -> KvinLevelDb.await(CompletableFuture.failedFuture(cause)));
		assertSame(cause, e);
	}

	@Test
	public void testErrorIsRethrown() {
		OutOfMemoryError cause = new OutOfMemoryError();
		OutOfMemoryError e = assertThrows(OutOfMemoryError.class,
				() -> KvinLevelDb.await(CompletableFuture.failedFuture(cause)));
		assertSame(cause, e);
	}

	@Test
	public void testInterruptIsWrapped() {
		Thread.currentThread().interrupt();
		KvinException e = assertThrows(KvinException.class,
				() -> KvinLevelDb.await(new CompletableFuture<>()));
		assertTrue(e.getCause() instanceof InterruptedException);
		// the interrupt status is restored
		assertTrue(Thread.interrupted());
		assertFalse(Thread.currentThread().isInterrupted());
	}
}
