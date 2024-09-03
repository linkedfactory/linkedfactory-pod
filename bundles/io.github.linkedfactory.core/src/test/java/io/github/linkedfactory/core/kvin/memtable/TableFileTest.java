package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.records.KvinRecord;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TableFileTest {
	private File tempDir;
	private Path tablePath;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("tablefile-test").toFile();
		tablePath = tempDir.toPath().resolve("table.data");
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(tempDir);
	}

	@Test
	public void shouldWriteAndReadRecords() throws IOException {
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(
					new KvinRecord(1L, 2L, 3L, 1000L, 1, "first".getBytes(StandardCharsets.UTF_8)),
					new KvinRecord(1L, 2L, 3L, 1002L, 0, "second".getBytes(StandardCharsets.UTF_8)),
					new KvinRecord(1L, 2L, 3L, 1002L, 1, "third".getBytes(StandardCharsets.UTF_8)),
					new KvinRecord(1L, 2L, 4L, 1003L, 0, "other-property".getBytes(StandardCharsets.UTF_8))
			);

			List<KvinRecord> records = tableFile.fetch(1L, 3L, 2L, 10L).toList();
			assertEquals(3, records.size());

			assertEquals(1002L, records.get(0).time());
			assertEquals(1, records.get(0).seqNr());
			assertArrayEquals("third".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(0).value());

			assertEquals(1002L, records.get(1).time());
			assertEquals(0, records.get(1).seqNr());
			assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(1).value());

			assertEquals(1000L, records.get(2).time());
			assertEquals(1, records.get(2).seqNr());
			assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(2).value());
		}
	}

	@Test
	public void shouldReadExistingFile() throws IOException {
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(
					new KvinRecord(11L, 22L, 33L, 2000L, 0, "persisted-1".getBytes(StandardCharsets.UTF_8)),
					new KvinRecord(11L, 22L, 33L, 1999L, 0, "persisted-2".getBytes(StandardCharsets.UTF_8))
			);
		}

		try (TableFile reopened = new TableFile(tablePath)) {
			List<KvinRecord> records = reopened.fetch(11L, 33L, 22L, 10L).toList();
			assertEquals(2, records.size());

			assertEquals(2000L, records.get(0).time());
			assertArrayEquals("persisted-1".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(0).value());

			assertEquals(1999L, records.get(1).time());
			assertArrayEquals("persisted-2".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(1).value());
		}
	}
}
