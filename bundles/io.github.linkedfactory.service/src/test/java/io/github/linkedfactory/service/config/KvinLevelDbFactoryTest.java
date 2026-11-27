package io.github.linkedfactory.service.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.*;

public class KvinLevelDbFactoryTest {
	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	private static KvinLevelDbFactory factory(String location) {
		return new KvinLevelDbFactory() {
			@Override
			public String getLocation() {
				return location;
			}
		};
	}

	@Test
	public void usesTheLocation() {
		String path = temporary.getRoot().getAbsolutePath();
		assertEquals(new File(path), factory(path).getStorePathOr("default"));
	}

	@Test
	public void acceptsFileUris() {
		File path = temporary.getRoot().getAbsoluteFile();
		assertEquals(path, factory(path.toURI().toString()).getStorePathOr("default"));
		assertEquals(path, factory("file://" + path).getStorePathOr("default"));
		var e = assertThrows(IllegalArgumentException.class,
				() -> factory("file:relative/store").getStorePathOr("default"));
		assertTrue(e.getMessage(), e.getMessage().contains("file:///"));
	}

	@Test
	public void rejectsUris() {
		var e = assertThrows(IllegalArgumentException.class,
				() -> factory("s3a://bucket/store").getStorePathOr("default"));
		assertTrue(e.getMessage(), e.getMessage().contains("local path"));
	}
}
