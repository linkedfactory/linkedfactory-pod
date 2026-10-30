package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import net.enilink.komma.core.URIs;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class IcebergPartitioningTest {
	private static final Comparator<Record> ORDER = Comparator
			.comparingLong((Record row) -> (Long) row.get(0))
			.thenComparingLong(row -> (Long) row.get(1)).thenComparingLong(row -> (Long) row.get(2))
			.thenComparing((a, b) -> Long.compare((Long) b.get(3), (Long) a.get(3)))
			.thenComparing((a, b) -> Integer.compare((Integer) b.get(4), (Integer) a.get(4)));

	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	private Path root() {
		return temporary.getRoot().toPath();
	}

	private Table table() {
		return new HadoopTables(new Configuration()).load(root().resolve("iceberg").toString());
	}

	private List<DataFile> files(Expression filter) throws Exception {
		Map<String, DataFile> result = new LinkedHashMap<>();
		try (CloseableIterable<FileScanTask> tasks = table().newScan().filter(filter).planFiles()) {
			for (FileScanTask task : tasks) {
				result.putIfAbsent(task.file().location(), task.file().copy());
			}
		}
		return new ArrayList<>(result.values());
	}

	private List<DataFile> files() throws Exception {
		return files(Expressions.alwaysTrue());
	}

	private void assertValidFiles() throws Exception {
		Table table = table();
		for (DataFile file : files()) {
			PartitionSpec spec = table.specs().get(file.specId());
			PartitionKey key = new PartitionKey(spec, table.schema());
			StructLikeWrapper expected = StructLikeWrapper.forType(spec.partitionType()).set(file.partition());
			Record previous = null;
			long count = 0;
			try (CloseableIterable<Record> rows = Parquet.read(table.io().newInputFile(file.location()))
					.project(table.schema())
					.createReaderFunc(schema -> GenericParquetReaders.buildReader(table.schema(), schema)).build()) {
				for (Record row : rows) {
					key.partition(row);
					assertEquals("Every row must belong to its file's partition", expected,
							StructLikeWrapper.forType(spec.partitionType()).set(key));
					boolean first = previous == null || !row.get(0).equals(previous.get(0))
							|| !row.get(1).equals(previous.get(1)) || !row.get(2).equals(previous.get(2));
					assertEquals(first, row.get(5));
					if (previous != null) {
						assertTrue(ORDER.compare(previous, row) <= 0);
					}
					previous = row.copy();
					count++;
				}
			}
			assertEquals(file.recordCount(), count);
			assertTrue(count > 0);
		}
	}

	@Test
	public void mixedPartitionsPreserveReadsLimitsPropertiesAndReopening() throws Exception {
		Function<Schema, PartitionSpec> partitioning = schema -> PartitionSpec.builderFor(schema)
				.bucket("itemId", 32).identity("contextId").truncate("time", 10).build();
		try (var store = new IcebergRecordStore(root().toString(), null, partitioning)) {
			List<KvinRecord> input = new ArrayList<>();
			for (long item : List.of(1L, 2L)) {
				for (long context : List.of(0L, 4L)) {
					for (long property : List.of(2L, 3L)) {
						for (long time : List.of(-11L, -1L, 0L, 9L, 10L, 19L, 20L)) {
							input.add(new KvinRecord(item, context, property, time, 0, time));
						}
					}
				}
			}
			store.put(input);
			assertTrue(files().size() > 1);
			assertValidFiles();
			List<DataFile> selected = files(Expressions.and(Expressions.equal("contextId", 0L),
					Expressions.equal("time", -1L)));
			assertFalse(selected.isEmpty());
			assertTrue(selected.size() < files().size());
			assertTrue(selected.stream().allMatch(file -> (Long) file.partition().get(2, Long.class) == -10L));
			try (var rows = store.fetch(1, null, null, 2)) {
				assertEquals(List.of(20L, 19L, 20L, 19L), rows.toList().stream().map(KvinRecord::time).toList());
			}
			try (var rows = store.fetch(1, 2L, null, 10, -1, 0, 0, null)) {
				assertEquals(List.of(10L, 9L, 0L, -1L), rows.toList().stream().map(KvinRecord::time).toList());
			}
			try (var properties = store.properties(1, null)) {
				assertEquals(List.of(2L, 3L), properties.toList());
			}
			try (var rows = store.fetch(List.of(1L, 2L), List.of(2L), 4L, 20, -11, 0, 0, null)) {
				assertEquals(14, rows.toList().size());
			}
		}
		try (var store = new IcebergRecordStore(root().toString())) {
			store.put(new KvinRecord(1, 0, 2, 21, 0, 21L));
			try (var rows = store.fetch(1, 2L, null, 1)) {
				assertEquals(21, rows.next().time());
			}
			assertValidFiles();
		}
		try (var ignored = new IcebergRecordStore(root().toString(), null, partitioning)) {
			assertTrue(table().spec().isPartitioned());
		}
	}

	@Test
	public void compactionThresholdAndDeleteRewritesArePartitionLocal() throws Exception {
		try (var store = new IcebergRecordStore(root().toString(), null,
				schema -> PartitionSpec.builderFor(schema).identity("propertyId").build())) {
			table().updateProperties().set("kvin.compaction.min-files", "3").commit();
			store.put(new KvinRecord(1, 0, 2, 0, 0, 0), new KvinRecord(1, 0, 3, 0, 0, 0));
			assertEquals(2, files().size());
			DataFile untouched = files(Expressions.equal("propertyId", 3L)).getFirst();
			store.put(new KvinRecord(1, 0, 2, 1, 0, 1));
			assertEquals("Threshold is not applied across partitions", 3, files().size());
			List<DataFile> oldFiles = files();
			store.put(new KvinRecord(1, 0, 2, 2, 0, 2));
			assertEquals(2, files().size());
			assertEquals(untouched.location(), files(Expressions.equal("propertyId", 3L)).getFirst().location());
			for (DataFile file : oldFiles) {
				assertTrue(Files.exists(Path.of(file.location())));
			}
			assertValidFiles();
			assertEquals(1, store.delete(1, 2L, null, 1, 1));
			try (var rows = store.fetch(1, 2L, null, 0)) {
				assertEquals(List.of(2L, 0L), rows.toList().stream().map(KvinRecord::time).toList());
			}
			assertValidFiles();
			try (var properties = store.properties(1, null)) {
				assertEquals(List.of(2L, 3L), properties.toList());
			}
			assertEquals(1, store.delete(1, 3L, null, Long.MAX_VALUE, Long.MIN_VALUE));
			try (var properties = store.properties(1, null)) {
				assertEquals(List.of(2L), properties.toList());
			}
		}
	}

	@Test
	public void explicitEvolutionKeepsOldSpecsReadableAndRepartitionsDeleteOutputs() throws Exception {
		try (var store = new IcebergRecordStore(root().toString())) {
			store.put(new KvinRecord(1, 0, 2, 0, 0, 0), new KvinRecord(1, 0, 2, 1, 0, 1),
					new KvinRecord(1, 0, 2, 11, 0, 11));
			table().updateSpec().addField("time_range", Expressions.truncate("time", 10)).commit();
			store.put(new KvinRecord(1, 0, 2, 21, 0, 21));
			assertEquals(2, files().size());
			assertEquals(1, store.delete(1, 2L, null, 0, 0));
			assertEquals(3, files().size());
			assertTrue(files().stream().allMatch(file -> file.specId() == table().spec().specId()));
			assertValidFiles();
			try (var rows = store.fetch(1, 2L, null, 0)) {
				assertEquals(List.of(21L, 11L, 1L), rows.toList().stream().map(KvinRecord::time).toList());
			}
		}
		try (var ignored = new IcebergRecordStore(root().toString(), null,
				schema -> PartitionSpec.builderFor(schema).truncate("time", 10, "time_range").build())) {
			assertTrue(table().spec().isPartitioned());
		}
	}

	@Test
	public void compactsMultiplePartitionsInOneCommitWithoutLosingDuplicates() throws Exception {
		try (var store = new IcebergRecordStore(root().toString(), null,
				schema -> PartitionSpec.builderFor(schema).identity("propertyId").build())) {
			table().updateProperties().set("kvin.compaction.min-files", "3").commit();
			for (int time = 0; time < 3; time++) {
				List<KvinRecord> batch = new ArrayList<>();
				for (long property : List.of(2L, 3L)) {
					batch.add(new KvinRecord(1, 0, property, time, 0, time));
					if (time == 0) {
						batch.add(new KvinRecord(1, 0, property, time, 0, time));
					}
				}
				store.put(batch);
			}
			assertEquals(2, files().size());
			assertTrue(files().stream().allMatch(file -> file.recordCount() == 4));
			assertValidFiles();
			try (var rows = store.fetch(1, null, null, 0)) {
				assertEquals(List.of(2L, 1L, 0L, 2L, 1L, 0L),
						rows.toList().stream().map(KvinRecord::time).toList());
			}
		}
	}

	@Test
	public void uncertainDeleteCommitRetainsPotentiallyCommittedPartitionFiles() throws Exception {
		try (var store = new IcebergRecordStore(root().toString(), null,
				schema -> PartitionSpec.builderFor(schema).identity("contextId").build())) {
			store.put(new KvinRecord(1, 0, 2, 1, 0, 1), new KvinRecord(1, 0, 2, 2, 0, 2));
			var field = IcebergRecordStore.class.getDeclaredField("table");
			field.setAccessible(true);
			Table storeTable = spy((Table) field.get(store));
			field.set(store, storeTable);
			RewriteFiles rewrite = spy(storeTable.newRewrite());
			doReturn(rewrite).when(storeTable).newRewrite();
			doAnswer(call -> {
				call.callRealMethod();
				throw new CommitStateUnknownException(new RuntimeException("Simulated lost commit response"));
			}).when(rewrite).commit();
			assertThrows(CommitStateUnknownException.class, () -> store.delete(1, 2L, null, 1, 1));
			assertValidFiles();
			try (var rows = store.fetch(1, 2L, null, 0)) {
				assertEquals(List.of(2L), rows.toList().stream().map(KvinRecord::time).toList());
			}
		}
	}

	@Test
	public void partitionWritersHandleBatchBoundaries() throws Exception {
		try (var store = new IcebergRecordStore(root().toString(), null,
				schema -> PartitionSpec.builderFor(schema).identity("contextId").build())) {
			List<KvinRecord> input = new ArrayList<>();
			for (int time = 0; time < 8200; time++) {
				input.add(new KvinRecord(1, time % 2, 2, time, 0, time));
			}
			store.put(input);
			assertEquals(4, files().size());
			assertValidFiles();
			for (long context : List.of(0L, 1L)) {
				try (var rows = store.fetch(1, 2L, context, 0)) {
					assertEquals(4100, rows.toList().size());
				}
			}
		}
	}

	@Test
	public void compactionPreservesHistoricalSpecs() throws Exception {
		try (var store = new IcebergRecordStore(root().toString())) {
			for (int time = 0; time < 3; time++) {
				store.put(new KvinRecord(1, 0, 2, time, 0, time));
			}
			int originalSpec = table().spec().specId();
			Table evolved = table();
			evolved.updateSpec().addField("contextId").commit();
			evolved.updateProperties().set("kvin.compaction.min-files", "3").commit();
			store.put(new KvinRecord(1, 0, 2, 3, 0, 3));
			assertEquals(2, files().size());
			assertEquals(1, files().stream().filter(file -> file.specId() == originalSpec).count());
			store.put(new KvinRecord(1, 0, 2, 4, 0, 4));
			assertEquals("Different spec IDs must not reach the threshold together", 3, files().size());
			store.put(new KvinRecord(1, 0, 2, 5, 0, 5));
			assertEquals(2, files().size());
			assertValidFiles();
			try (var rows = store.fetch(1, 2L, null, 0)) {
				assertEquals(List.of(5L, 4L, 3L, 2L, 1L, 0L), rows.toList().stream().map(KvinRecord::time).toList());
			}
		}
	}

	@Test
	public void defaultsAndExplicitConfigurationAreValidated() throws Exception {
		try (var ignored = new IcebergRecordStore(root().toString())) {
			assertTrue(table().spec().isUnpartitioned());
		}
		assertThrows(IllegalArgumentException.class, () -> new IcebergRecordStore(root().toString(), null,
				schema -> PartitionSpec.builderFor(schema).bucket("itemId", 4).build()));
		assertThrows(IllegalArgumentException.class, () -> new IcebergRecordStore(root().toString(), null,
				schema -> PartitionSpec.builderFor(schema).identity("first").build()));
		assertThrows(NullPointerException.class, () -> new IcebergRecordStore(root().toString(), null, schema -> null));
		try (var ignored = new IcebergRecordStore(root().toString(), null, schema -> PartitionSpec.unpartitioned())) {
			assertTrue(table().spec().isUnpartitioned());
		}
	}

	@Test
	public void facadeOwnsPartitionedStoresAndPreservesUriMappings() throws Exception {
		var item = URIs.createURI("urn:partition:item");
		var property = URIs.createURI("urn:partition:property");
		try (var store = new KvinIceberg(root().toString(), null,
				schema -> PartitionSpec.builderFor(schema).bucket("itemId", 8).truncate("time", 10).build())) {
			store.put(new KvinTuple(item, property, null, 1, 1), new KvinTuple(item, property, null, 11, 11));
			assertEquals(2, files().size());
			assertValidFiles();
		}
		try (var store = new KvinIceberg(root().toString())) {
			try (var rows = store.fetch(item, property, null, 0)) {
				assertEquals(List.of(11L, 1L), rows.toList().stream().map(tuple -> tuple.time).toList());
			}
		}
	}
}
