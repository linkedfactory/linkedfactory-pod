package io.github.linkedfactory.core.kvin.leveldb;

import net.enilink.komma.core.URIs;
import org.junit.After;
import org.junit.Test;

import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;

public class KvinLevelDbBaseTest {
	final KvinLevelDbBase base = new KvinLevelDbBase() {
	};

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
	public void testNoTtl() {
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?a=b"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?xttl=5s"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=5sx"));
		assertEquals(OptionalLong.empty(), ttl("http://example.org/item?ttl=5h"));
	}
}
