package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.ids.IdKind;
import io.github.linkedfactory.core.kvin.ids.IdStore;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class IcebergIdStoreTest {
	private Path directory;
	private IcebergIdStore store;

	@Before
	public void setUp() throws Exception {
		directory = Path.of("target", "iceberg-id-tests", UUID.randomUUID().toString()).toAbsolutePath();
		Files.createDirectories(directory);
		store = new IcebergIdStore(directory.toString());
	}

	@After
	public void tearDown() throws Exception {
		store.close();
		FileUtils.deleteDirectory(directory.toFile());
	}

	@Test
	public void bulkInterfaceResolvesCachedPendingAndPersistedMappingsForEveryKind() {
		URI first = URIs.createURI("urn:ids:first");
		URI second = URIs.createURI("urn:ids:second");
		URI missing = URIs.createURI("urn:ids:missing");
		IdStore ids = store;
		for (IdKind kind : IdKind.values()) {
			assertTrue(ids.getIds(List.of(), kind, true).isEmpty());
			assertTrue(ids.getUris(List.of(), kind).isEmpty());
			assertEquals(Map.of(first, 1L), ids.getIds(List.of(first, first), kind, true));
			store.flush();
			try (var reader = new IcebergIdStore(directory.toString())) {
				assertEquals(Map.of(first, 1L), reader.getIds(List.of(first, missing, first), kind, false));
				assertEquals(Map.of(1L, first), reader.getUris(List.of(1L, 1L), kind));
			}
			assertEquals(Map.of(first, 1L, second, 2L),
					ids.getIds(List.of(first, second, second), kind, true));
			assertEquals(Map.of(1L, first, 2L, second), ids.getUris(List.of(2L, 1L, 2L), kind));
			assertThrows(IllegalStateException.class, () -> ids.getUris(List.of(3L), kind));
			store.flush();
		}
	}

	@Test
	public void stagesAllKindsAndFlushesDurableMappingsWithoutARecordTable() {
		Map<IdKind, URI> uris = new EnumMap<>(IdKind.class);
		for (IdKind kind : IdKind.values()) {
			URI uri = URIs.createURI("urn:ids:" + kind);
			uris.put(kind, uri);
			assertEquals(0, store.getId(uri, kind, false));
			long id = store.getId(uri, kind, true);
			assertEquals(1, id);
			assertEquals(id, store.getId(uri, kind, false));
			assertEquals(uri, store.getUri(id, kind));
		}
		HadoopTables tables = new HadoopTables(new Configuration());
		for (String name : List.of("items", "contexts", "properties")) {
			assertNull(tables.load(directory.resolve("iceberg-ids").resolve(name).toString()).currentSnapshot());
		}
		store.flush();
		store.close();
		store = new IcebergIdStore(directory.toString());
		for (var entry : uris.entrySet()) {
			assertEquals(1, store.getId(entry.getValue(), entry.getKey(), false));
			assertEquals(entry.getValue(), store.getUri(1, entry.getKey()));
		}
		assertEquals(0, store.getId(uris.get(IdKind.PROPERTY), IdKind.CONTEXT, false));
		assertEquals(0, store.getId(uris.get(IdKind.CONTEXT), IdKind.PROPERTY, false));
		assertFalse(Files.exists(directory.resolve("iceberg")));
	}

	@Test
	public void independentInstancesCoordinateStagedAllocationsAndRefreshMisses() {
		URI first = URIs.createURI("urn:ids:first");
		URI second = URIs.createURI("urn:ids:second");
		try (var other = new IcebergIdStore(directory.toString())) {
			assertEquals(0, other.getId(first, IdKind.ITEM, false));
			long firstId = store.getId(first, IdKind.ITEM, true);
			long secondId = other.getId(second, IdKind.ITEM, true);
			assertNotEquals(firstId, secondId);
			assertEquals(firstId, other.getId(first, IdKind.ITEM, true));
			other.flush();
			assertEquals(secondId, store.getId(second, IdKind.ITEM, false));
			assertEquals(second, store.getUri(secondId, IdKind.ITEM));
			assertEquals(firstId, store.getId(first, IdKind.ITEM, true));
			store.flush();
		}
	}

	@Test
	public void concurrentAllocatorsNeverDuplicateIdsOrUriMappings() throws Exception {
		try (var other = new IcebergIdStore(directory.toString());
			 var executor = Executors.newFixedThreadPool(2)) {
			var first = executor.submit(() -> allocate(store, "first"));
			var second = executor.submit(() -> allocate(other, "second"));
			List<Long> ids = new ArrayList<>(first.get(30, TimeUnit.SECONDS));
			ids.addAll(second.get(30, TimeUnit.SECONDS));
			assertEquals(ids.size(), new HashSet<>(ids).size());
			assertEquals(store.getId(URIs.createURI("urn:ids:shared"), IdKind.PROPERTY, false),
					other.getId(URIs.createURI("urn:ids:shared"), IdKind.PROPERTY, false));
		}
	}

	private List<Long> allocate(IcebergIdStore ids, String prefix) {
		List<Long> result = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			result.add(ids.getId(URIs.createURI("urn:ids:" + prefix + ":" + i), IdKind.ITEM, true));
			ids.getId(URIs.createURI("urn:ids:shared"), IdKind.PROPERTY, true);
			ids.flush();
		}
		return result;
	}

	@Test
	public void automaticFlushBoundsPendingMappingsAndCloseFlushesTheRemainder() throws Exception {
		List<URI> uris = new ArrayList<>();
		for (int i = 0; i < 8193; i++) {
			uris.add(URIs.createURI("urn:ids:batch:" + i));
		}
		Map<URI, Long> mappings = store.resolveIds(uris, IdKind.ITEM, true);
		assertEquals(8193, mappings.size());
		var table = new HadoopTables(new Configuration()).load(directory.resolve("iceberg-ids/items").toString());
		assertEquals("8192", table.currentSnapshot().summary().get("total-records"));
		assertEquals(uris.getLast(), store.getUri(mappings.get(uris.getLast()), IdKind.ITEM));
		store.close();
		store = new IcebergIdStore(directory.toString());
		assertEquals(mappings, store.resolveIds(uris, IdKind.ITEM, false));
		table.refresh();
		try (var files = table.newScan().planFiles()) {
			assertEquals(2, java.util.stream.StreamSupport.stream(files.spliterator(), false).count());
		}
	}
}
