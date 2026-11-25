package io.github.linkedfactory.core.rdf4j.fts;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class LuceneFtsIndexRegistryTest {
	private Path indexDir;

	@Before
	public void setUp() throws IOException {
		indexDir = Files.createTempDirectory("lucene-fts-registry-");
	}

	@After
	public void tearDown() throws IOException {
		LuceneFtsIndexRegistry.release(indexDir.toString());
		deleteRecursively(indexDir);
	}

	@Test
	public void getOrCreateReturnsTheSameInstanceForTheSameDirectory() throws IOException {
		LuceneFtsIndex first = LuceneFtsIndexRegistry.getOrCreate(indexDir.toString());
		LuceneFtsIndex second = LuceneFtsIndexRegistry.getOrCreate(indexDir.toString());
		assertSame(first, second);
		LuceneFtsIndexRegistry.release(indexDir.toString());
		LuceneFtsIndexRegistry.release(indexDir.toString());
	}

	@Test
	public void releaseDoesNotCloseTheSharedIndexWhileReferencesRemain() throws IOException {
		LuceneFtsIndex first = LuceneFtsIndexRegistry.getOrCreate(indexDir.toString());
		LuceneFtsIndexRegistry.getOrCreate(indexDir.toString());

		LuceneFtsIndexRegistry.release(indexDir.toString());

		first.commit();

		LuceneFtsIndexRegistry.release(indexDir.toString());
	}

	@Test
	public void releasingTheLastReferenceClosesTheIndexAndAllowsAFreshOneAfterward() throws IOException {
		LuceneFtsIndex first = LuceneFtsIndexRegistry.getOrCreate(indexDir.toString());

		LuceneFtsIndexRegistry.release(indexDir.toString());

		assertThrows(Exception.class, first::commit);

		LuceneFtsIndex second = LuceneFtsIndexRegistry.getOrCreate(indexDir.toString());
		assertNotSame(first, second);
		LuceneFtsIndexRegistry.release(indexDir.toString());
	}

	private static void deleteRecursively(Path dir) throws IOException {
		if (!Files.exists(dir)) {
			return;
		}
		try (var stream = Files.walk(dir)) {
			stream.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
				try {
					Files.deleteIfExists(p);
				} catch (IOException ignored) {
				}
			});
		}
	}
}
