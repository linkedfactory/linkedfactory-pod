package io.github.linkedfactory.service.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.*;

public class KvinLevelDbFactoryTest {
	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	private static KvinLevelDbFactory factory(String location, String dirName) {
		return new KvinLevelDbFactory() {
			@Override
			public String getLocation() {
				return location;
			}

			@Override
			public String getDirName() {
				return dirName;
			}
		};
	}

	@Test
	public void usesTheLocationOrTheDeprecatedDirName() {
		String path = temporary.getRoot().getAbsolutePath();
		assertEquals(new File(path), factory(path, null).getStorePathOr("default"));
		assertEquals(new File(path), factory(null, path).getStorePathOr("default"));
		assertThrows(IllegalArgumentException.class, () -> factory(path, path).getStorePathOr("default"));
	}

	@Test
	public void acceptsFileUris() {
		File path = temporary.getRoot().getAbsoluteFile();
		assertEquals(path, factory(path.toURI().toString(), null).getStorePathOr("default"));
		assertEquals(path, factory("file://" + path, null).getStorePathOr("default"));
		var e = assertThrows(IllegalArgumentException.class,
				() -> factory("file:relative/store", null).getStorePathOr("default"));
		assertTrue(e.getMessage(), e.getMessage().contains("file:///"));
	}

	@Test
	public void rejectsUris() {
		var e = assertThrows(IllegalArgumentException.class,
				() -> factory("s3a://bucket/store", null).getStorePathOr("default"));
		assertTrue(e.getMessage(), e.getMessage().contains("local path"));
	}
}
