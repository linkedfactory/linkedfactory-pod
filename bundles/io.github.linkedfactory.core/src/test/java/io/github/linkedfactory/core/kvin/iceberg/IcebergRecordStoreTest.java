package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.ids.IdKind;
import io.github.linkedfactory.core.kvin.ids.IdStore;
import io.github.linkedfactory.core.kvin.parquet.Records;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.records.KvinRecordStore;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class IcebergRecordStoreTest {
	private Path directory;
	private IcebergRecordStore store;

	@Before
	public void setUp() throws Exception {
		directory = Path.of("target", "iceberg-record-tests", UUID.randomUUID().toString()).toAbsolutePath();
		Files.createDirectories(directory);
		store = new IcebergRecordStore(directory.toString());
	}

	@After
	public void tearDown() throws Exception {
		store.close();
		FileUtils.deleteDirectory(directory.toFile());
	}

	@Test
	public void standaloneRecordsPreserveTypedValuesAfterReopeningWithoutMappings() throws Exception {
		URI uri = URIs.createURI("urn:standalone:value");
		Record object = new Record(uri, "object");
		Object[] array = {1, uri, object};
		List<Object> values = Arrays.asList(1, 2L, 3.5f, 4.5, "text", true, null,
				(short) 7, new BigInteger("1234567890123456789012345"), new BigDecimal("1234.5678"),
				uri, object, array, Records.encodeRecord(object), ByteBuffer.wrap(Records.encodeRecord(uri)));
		List<KvinRecord> input = new ArrayList<>();
		for (int i = 0; i < values.size(); i++) {
			input.add(new KvinRecord(7, 0, 9, -i, i, values.get(i)));
		}
		store.put(input);
		store.close();
		store = new IcebergRecordStore(directory.toString());
		assertFalse(Files.exists(directory.resolve("iceberg-ids")));
		try (var rows = store.fetch(7, 9L, null, 0)) {
			List<KvinRecord> actual = rows.toList();
			assertEquals(values.size(), actual.size());
			for (int i = 0; i < values.size(); i++) {
				assertEquals(-i, actual.get(i).time());
				assertEquals(i, actual.get(i).seqNr());
				Object expected = i == 13 ? object : i == 14 ? uri : values.get(i);
				if (expected instanceof BigInteger number) {
					expected = number.longValue();
				} else if (expected instanceof BigDecimal number) {
					expected = number.doubleValue();
				}
				if (expected instanceof Object[] elements) {
					assertArrayEquals(elements, (Object[]) actual.get(i).value());
				} else {
					assertEquals(expected, actual.get(i).value());
				}
			}
		}
	}

	@Test
	public void wildcardPropertiesLimitsAggregationAndDeletesUseNumericKeys() {
		store.put(new KvinRecord(1, 0, 2, 29, 0, 4),
				new KvinRecord(1, 0, 2, 21, 0, 2),
				new KvinRecord(1, 0, 2, 19, 0, 99),
				new KvinRecord(1, 0, 3, 29, 0, 8),
				new KvinRecord(1, 0, 3, 21, 0, 2),
				new KvinRecord(1, 4, 2, 30, 0, "other context"),
				new KvinRecord(2, 0, 2, -1, 0, "other item"));
		store.put(new KvinRecord(1, 0, 2, 29, 0, 4));
		try (var properties = store.properties(1, null)) {
			assertEquals(List.of(2L, 3L), properties.toList());
		}
		for (Long property : Arrays.asList(null, -1L)) {
			try (var rows = store.fetch(1, property, null, 1)) {
				assertEquals(List.of(2L, 3L), rows.toList().stream().map(KvinRecord::propertyId).toList());
			}
		}
		for (List<Long> properties : List.of(List.<Long>of(), KvinRecordStore.NULL_PROPERTY)) {
			try (var rows = store.fetch(List.of(1L), properties, null, 29, 0, 1, 10, " SUM ")) {
				List<KvinRecord> aggregated = rows.toList();
				assertEquals(List.of(6, 10), aggregated.stream().map(KvinRecord::value).toList());
				assertEquals(List.of(20L, 20L), aggregated.stream().map(KvinRecord::time).toList());
				assertEquals(List.of(2L, 3L), aggregated.stream().map(KvinRecord::propertyId).toList());
			}
		}
		try (var snapshot = store.fetch(1, 2L, null, 0)) {
			assertEquals(2, store.delete(1, 2L, null, 29, 29));
			assertEquals(3, snapshot.toList().size());
		}
		assertTrue(store.delete(1, null));
		assertFalse(store.delete(1, null));
		try (var properties = store.properties(1, null)) {
			assertFalse(properties.hasNext());
		}
		try (var rows = store.fetch(1, 2L, 4L, 0)) {
			assertEquals("other context", rows.next().value());
		}
		try (var rows = store.fetch(2, null, null, 0)) {
			assertEquals(-1, rows.next().time());
		}
		assertEquals(1, store.delete(1, -1L, 4L, Long.MAX_VALUE, Long.MIN_VALUE));
	}

	@Test
	public void facadeComposesCallerOwnedComponentsAndRetainsUnboundedLatestFetch() {
		URI item = URIs.createURI("urn:standalone:item");
		URI property = URIs.createURI("urn:standalone:property");
		try (var ids = new IcebergIdStore(directory.toString())) {
			try (var facade = new KvinIceberg(store, ids)) {
				facade.put(new KvinTuple(item, property, null, -1, 0, "value"));
				try (var rows = facade.fetch(item, property, null, 1)) {
					assertEquals(-1, rows.next().time);
					assertFalse(rows.hasNext());
				}
				facade.createBranch("standalone");
				assertTrue(facade.delete(item, null));
				assertFalse(facade.delete(item, null));
			}
			assertTrue(ids.getId(item, IdKind.ITEM, false) > 0);
			store.put(new KvinRecord(42, 0, 43, -2, 0, 44));
			try (var rows = store.fetch(42, 43L, null, 1)) {
				assertEquals(44, rows.next().value());
			}
		}
	}

	@Test
	public void facadeFlushesMappingsBeforeHandingRecordsToAnInjectedStore() {
		KvinRecordStore records = mock(KvinRecordStore.class);
		try (var ids = new IcebergIdStore(directory.toString())) {
			doAnswer(call -> {
				Iterable<KvinRecord> input = call.getArgument(0);
				int count = 0;
				for (KvinRecord record : input) {
					for (String name : List.of("items", "contexts", "properties")) {
						var table = new HadoopTables(new Configuration()).load(directory.resolve("iceberg-ids").resolve(name).toString());
						assertNotNull("Mappings must already be committed", table.currentSnapshot());
					}
					assertTrue(record.itemId() > 0);
					count++;
				}
				assertEquals(1, count);
				return null;
			}).when(records).put(any(Iterable.class));
			try (var facade = new KvinIceberg(records, ids)) {
				facade.put(new KvinTuple(URIs.createURI("urn:item"), URIs.createURI("urn:property"), null, 1, 0, 1));
			}
			verify(records, never()).close();
		}
		IdStore genericIds = mock(IdStore.class);
		when(genericIds.getId(any(), any(), eq(true))).thenReturn(1L);
		KvinRecordStore genericRecords = mock(KvinRecordStore.class);
		doAnswer(call -> {
			for (KvinRecord ignored : (Iterable<KvinRecord>) call.getArgument(0)) {
				verify(genericIds).flush();
			}
			return null;
		}).when(genericRecords).put(any(Iterable.class));
		try (var facade = new KvinIceberg(genericRecords, genericIds)) {
			facade.put(new KvinTuple(URIs.createURI("urn:item"), URIs.createURI("urn:property"), null, 1, 0, 1));
			assertThrows(UnsupportedOperationException.class, () -> facade.createBranch("unsupported"));
		}
		verify(genericRecords, never()).close();
	}
}
