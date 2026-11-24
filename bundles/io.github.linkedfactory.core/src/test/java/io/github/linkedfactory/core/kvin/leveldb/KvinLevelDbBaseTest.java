package io.github.linkedfactory.core.kvin.leveldb;

import io.github.linkedfactory.core.kvin.util.Varint;
import net.enilink.komma.core.URIs;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalLong;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KvinLevelDbBaseTest {
	final KvinLevelDbBase base = new KvinLevelDbBase() {
	};

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@After
	public void shutdown() {
		base.executor.shutdown();
	}

	OptionalLong ttl(String uri) {
		return base.ttl(URIs.createURI(uri));
	}

	@Test
	public void testTtlUnits() {
		assertEquals(OptionalLong.of(5L), ttl("http://example.org/item?ttl=5000000ns"));
		assertEquals(OptionalLong.of(5L), ttl("http://example.org/item?ttl=5000us"));
		assertEquals(OptionalLong.of(5L), ttl("http://example.org/item?ttl=5ms"));
		assertEquals(OptionalLong.of(5000L), ttl("http://example.org/item?ttl=5s"));
		assertEquals(OptionalLong.of(5L * 60 * 1000), ttl("http://example.org/item?ttl=5m"));
		assertEquals(OptionalLong.of(2L * 24 * 60 * 60 * 1000), ttl("http://example.org/item?ttl=2d"));
	}

	@Test
	public void testTtlWithOtherQueryParameters() {
		assertEquals(OptionalLong.of(5000L), ttl("http://example.org/item?a=b&ttl=5s"));
		assertEquals(OptionalLong.of(5000L), ttl("http://example.org/item?ttl=5s&a=b"));
		assertEquals(OptionalLong.of(5000L), ttl("http://example.org/item?a=b&ttl=5s&c=d"));
	}

	@Test
	public void testZeroTtl() {
		for (String unit : new String[]{"ns", "us", "ms", "s", "m", "d"}) {
			assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=0" + unit));
		}
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=00s"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?a=b&ttl=0s"));
		// less than one millisecond
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=999999ns"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=999us"));
		assertEquals(OptionalLong.of(1L), ttl("http://example.org/item?ttl=1000us"));
		assertEquals(OptionalLong.of(1L), ttl("http://example.org/item?ttl=1ms"));
	}

	@Test
	public void testTtlOverflow() {
		long max = Long.MAX_VALUE;
		assertEquals(OptionalLong.of(max), ttl("http://example.org/item?ttl=" + max + "ms"));
		assertEquals(OptionalLong.of(max / 1000000L), ttl("http://example.org/item?ttl=" + max + "ns"));
		assertEquals(OptionalLong.of(max / 1000 * 1000), ttl("http://example.org/item?ttl=" + max / 1000 + "s"));

		// the number does not fit into a long value
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=" + max + "0ms"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=" + max + "0ns"));
		// the number fits, but not its conversion to milliseconds
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=" + (max / 1000 + 1) + "s"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=" + (max / 60000 + 1) + "m"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=999999999999d"));
	}

	@Test
	public void testNoTtl() {
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?a=b"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?xttl=5s"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=5sx"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=5h"));
	}

	byte[] key(byte[] prefix, long time) {
		ByteBuffer bb = ByteBuffer.allocate(prefix.length + Varint.MAX_BYTES);
		bb.put(prefix);
		base.writeVarint(bb, time);
		byte[] key = new byte[bb.position()];
		bb.flip().get(key);
		return key;
	}

	@Test
	public void testRemoveByTtl() throws IOException {
		byte[] prefix = {1};
		byte[] otherPrefix = {2};
		long now = System.currentTimeMillis();
		try (DB db = factory.open(tempFolder.newFolder(), new Options().createIfMissing(true))) {
			byte[] old = key(prefix, now - 10000);
			byte[] recent = key(prefix, now - 100);
			byte[] other = key(otherPrefix, now - 10000);
			for (byte[] k : new byte[][]{old, recent, other}) {
				db.put(k, new byte[]{0});
			}

			base.removeByTtl(db, prefix, 5000);
			assertNull(db.get(old));
			assertNotNull(db.get(recent));
			assertNotNull(db.get(other));
		}
	}

	@Test
	public void testRemoveByTtlLargerThanNow() throws IOException {
		byte[] prefix = {1};
		byte[] otherPrefix = {2};
		try (DB db = factory.open(tempFolder.newFolder(), new Options().createIfMissing(true))) {
			byte[][] keys = {key(prefix, 0), key(prefix, 1000), key(prefix, System.currentTimeMillis()),
					key(otherPrefix, 1000)};
			for (byte[] k : keys) {
				db.put(k, new byte[]{0});
			}

			long now = System.currentTimeMillis();
			for (long ttl : new long[]{now + 1, now + 86400000L, Long.MAX_VALUE}) {
				base.removeByTtl(db, prefix, ttl);
				for (byte[] k : keys) {
					assertNotNull(db.get(k));
				}
			}
		}
	}
}
