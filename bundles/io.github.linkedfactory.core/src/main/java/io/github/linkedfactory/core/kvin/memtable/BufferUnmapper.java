package io.github.linkedfactory.core.kvin.memtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Releases the memory of mapped buffers immediately instead of when the buffers are garbage
 * collected. Uses {@code sun.misc.Unsafe.invokeCleaner}, as Java 21 has no public API for it.
 * If that method is not available, for example because an OSGi framework does not delegate
 * {@code sun.misc} to the boot class loader, the buffers are released by the garbage collector.
 * <p>
 * Any access to an unmapped buffer crashes the JVM, so callers must ensure that a buffer is
 * not used after it was unmapped.
 */
final class BufferUnmapper {
	private static final Logger log = LoggerFactory.getLogger(BufferUnmapper.class);
	private static final MethodHandle INVOKE_CLEANER = findInvokeCleaner();

	private BufferUnmapper() {
	}

	private static MethodHandle findInvokeCleaner() {
		try {
			Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
			Field field = unsafeClass.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			Object unsafe = field.get(null);
			return MethodHandles.lookup()
					.unreflect(unsafeClass.getMethod("invokeCleaner", ByteBuffer.class))
					.bindTo(unsafe);
		} catch (ReflectiveOperationException | RuntimeException e) {
			log.info("Mapped table files are released by the garbage collector, as they cannot be unmapped: {}",
					e.toString());
			return null;
		}
	}

	/**
	 * True if buffers are unmapped immediately.
	 */
	static boolean isSupported() {
		return INVOKE_CLEANER != null;
	}

	/**
	 * Unmaps the buffer if supported.
	 *
	 * @return true if the buffer was unmapped
	 */
	static boolean unmap(MappedByteBuffer buffer) {
		if (INVOKE_CLEANER == null) {
			return false;
		}
		try {
			INVOKE_CLEANER.invokeExact((ByteBuffer) buffer);
			return true;
		} catch (Throwable e) {
			log.warn("Unmapping a table file failed, it is released by the garbage collector", e);
			return false;
		}
	}
}
