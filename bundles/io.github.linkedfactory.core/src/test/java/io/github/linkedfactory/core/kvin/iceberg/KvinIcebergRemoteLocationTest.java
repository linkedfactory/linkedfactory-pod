package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * A store at a remote location, through a Hadoop file system that is not the local one.
 */
public class KvinIcebergRemoteLocationTest {
	private final URI item = URIs.createURI("urn:remote:item");
	private final URI property = URIs.createURI("urn:remote:property");

	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	private String location() {
		return RemoteTestFileSystem.SCHEME + ":" + temporary.getRoot() + "/archive/";
	}

	private Table table() {
		return new HadoopTables(new Configuration()).load(RemoteTestFileSystem.SCHEME + ":" + temporary.getRoot() + "/archive/iceberg");
	}

	private List<String> dataFiles() throws Exception {
		List<String> locations = new ArrayList<>();
		try (CloseableIterable<FileScanTask> tasks = table().newScan().planFiles()) {
			for (FileScanTask task : tasks) {
				locations.add(task.file().location());
			}
		}
		return locations;
	}

	private List<Long> times(KvinIceberg store) {
		try (IExtendedIterator<KvinTuple> tuples = store.fetch(item, property, null, 0)) {
			return tuples.toList().stream().map(tuple -> tuple.time).toList();
		}
	}

	@Test
	public void writesCompactsDeletesAndReopens() throws Exception {
		try (KvinIceberg store = new KvinIceberg(location())) {
			table().updateProperties().set("kvin.compaction.min-files", "2").commit();
			store.put(new KvinTuple(null, item, property, 1, 0, 1L));
			store.put(new KvinTuple(null, item, property, 2, 0, 2L));
			((IcebergRecordStore) store.getRecordStore()).awaitCompaction();
			List<String> files = dataFiles();
			assertEquals(1, files.size());
			assertTrue("Data files are written through the remote file system: " + files,
					files.getFirst().startsWith(RemoteTestFileSystem.SCHEME + ":"));
			assertEquals(List.of(2L, 1L), times(store));
			assertTrue(store.delete(item, property, null, 1, 1) > 0);
			assertEquals(List.of(2L), times(store));
		}
		assertFalse("A remote archive has no ID lock file",
				Files.exists(temporary.getRoot().toPath().resolve("archive/iceberg-ids.lock")));
		try (KvinIceberg store = new KvinIceberg(location())) {
			assertEquals(List.of(2L), times(store));
			store.put(new KvinTuple(null, item, property, 3, 0, 3L));
			assertEquals(List.of(3L, 2L), times(store));
		}
	}
}
