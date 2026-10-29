package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.ids.IdKind;
import io.github.linkedfactory.core.kvin.ids.IdStore;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.records.KvinRecordStore;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KvinIcebergBatchLookupTest {
	private final URI item = URIs.createURI("urn:batch:item");
	private final URI first = URIs.createURI("urn:batch:first");
	private final URI second = URIs.createURI("urn:batch:second");
	private final URI third = URIs.createURI("urn:batch:third");
	private IdStore ids;
	private KvinRecordStore records;
	private KvinIceberg store;

	@Before
	public void setUp() {
		ids = mock(IdStore.class);
		records = mock(KvinRecordStore.class);
		store = new KvinIceberg(records, ids);
		when(ids.getIds(List.of(item), IdKind.ITEM, false)).thenReturn(Map.of(item, 1L));
		when(ids.getId(Kvin.DEFAULT_CONTEXT, IdKind.CONTEXT, false)).thenReturn(1L);
		when(ids.getIds(List.of(), IdKind.PROPERTY, false)).thenReturn(Map.of());
	}

	private IExtendedIterator<KvinRecord> rows(List<KvinRecord> rows, long limit) {
		IExtendedIterator<KvinRecord> iterator = spy(WrappedIterator.create(rows.iterator()));
		when(records.fetch(1L, null, 1L, limit)).thenReturn(iterator);
		return iterator;
	}

	@Test
	public void boundsReadAheadDeduplicatesAndReusesMappingsAcrossChunks() {
		List<KvinRecord> input = new ArrayList<>();
		for (int i = 0; i < 2050; i++) {
			input.add(new KvinRecord(1, 1, i < 1024 ? i % 2 + 1 : 3, 2050 - i, 0, i));
		}
		var base = rows(input, 0);
		when(ids.getUris(Set.of(1L, 2L), IdKind.PROPERTY)).thenReturn(Map.of(1L, first, 2L, second));
		when(ids.getUris(Set.of(3L), IdKind.PROPERTY)).thenReturn(Map.of(3L, third));
		try (var values = store.fetch(item, null, null, 0)) {
			verify(base, never()).next();
			assertTrue(values.hasNext());
			assertTrue(values.hasNext());
			verify(base, times(1024)).next();
			List<KvinTuple> output = values.toList();
			assertEquals(input.size(), output.size());
			for (int i = 0; i < output.size(); i++) {
				assertEquals(i, output.get(i).value);
				assertEquals(2050 - i, output.get(i).time);
				assertEquals(item, output.get(i).item);
				assertEquals(Kvin.DEFAULT_CONTEXT, output.get(i).context);
				assertEquals(i < 1024 ? (i % 2 == 0 ? first : second) : third, output.get(i).property);
			}
		}
		verify(ids, times(1)).getUris(Set.of(1L, 2L), IdKind.PROPERTY);
		verify(ids, times(1)).getUris(Set.of(3L), IdKind.PROPERTY);
		verify(ids, never()).getUri(anyLong(), any());
		verify(base, times(1)).close();
	}

	@Test
	public void precedingUrisFollowIndependentIdChangesAndRevisitedSeries() {
		URI otherItem = URIs.createURI("urn:batch:other-item");
		List<URI> items = List.of(item, otherItem);
		List<URI> properties = List.of(first, second);
		when(ids.getIds(items, IdKind.ITEM, false)).thenReturn(Map.of(item, 1001L, otherItem, 1002L));
		when(ids.getIds(properties, IdKind.PROPERTY, false)).thenReturn(Map.of(first, 2001L, second, 2002L));
		List<KvinRecord> input = List.of(
				new KvinRecord(1001, 1, 2001, 6, 0, 0),
				new KvinRecord(1001, 1, 2001, 5, 1, 1),
				new KvinRecord(1002, 1, 2001, 4, 2, 2),
				new KvinRecord(1002, 1, 2002, 3, 3, 3),
				new KvinRecord(1001, 1, 2002, 2, 4, 4),
				new KvinRecord(1001, 1, 2001, 1, 5, 5));
		when(records.fetch(anyList(), anyList(), eq(1L), eq(6L), eq(0L), eq(0L), eq(0L), isNull()))
				.thenReturn(WrappedIterator.create(input.iterator()));
		try (var values = store.fetch(items, properties, null, 6, 0, 0, 0, null)) {
			for (KvinRecord row : input) {
				assertTrue(values.hasNext());
				assertTrue(values.hasNext());
				KvinTuple tuple = values.next();
				assertEquals(row.itemId() == 1001 ? item : otherItem, tuple.item);
				assertEquals(row.propertyId() == 2001 ? first : second, tuple.property);
				assertEquals(Kvin.DEFAULT_CONTEXT, tuple.context);
				assertEquals(row.time(), tuple.time);
				assertEquals(row.seqNr(), tuple.seqNr);
				assertEquals(row.value(), tuple.value);
			}
			assertFalse(values.hasNext());
		}
		verify(ids, never()).getUris(anyCollection(), any());
	}

	@Test
	public void precedingUrisRemainCorrectAcrossReadBatches() {
		List<KvinRecord> input = new ArrayList<>();
		for (int i = 0; i < 2050; i++) {
			input.add(new KvinRecord(1, 1, i < 1025 || i >= 2048 ? 1 : 2, 2050 - i, 0, i));
		}
		rows(input, 0);
		when(ids.getUris(Set.of(1L), IdKind.PROPERTY)).thenReturn(Map.of(1L, first));
		when(ids.getUris(Set.of(2L), IdKind.PROPERTY)).thenReturn(Map.of(2L, second));
		try (var values = store.fetch(item, null, null, 0)) {
			for (int i = 0; i < input.size(); i++) {
				KvinTuple tuple = values.next();
				assertEquals(item, tuple.item);
				assertEquals(i < 1025 || i >= 2048 ? first : second, tuple.property);
				assertEquals(i, tuple.value);
			}
			assertFalse(values.hasNext());
		}
		verify(ids, times(1)).getUris(Set.of(1L), IdKind.PROPERTY);
		verify(ids, times(1)).getUris(Set.of(2L), IdKind.PROPERTY);
	}

	@Test
	public void unknownPropertyAfterCachedPropertyStillClosesReader() {
		var base = rows(List.of(new KvinRecord(1, 1, 1, 2, 0, 2),
				new KvinRecord(1, 1, 2, 1, 0, 1)), 0);
		when(ids.getUris(Set.of(1L, 2L), IdKind.PROPERTY)).thenReturn(Map.of(1L, first));
		try (var values = store.fetch(item, null, null, 0)) {
			assertEquals(first, values.next().property);
			var failure = assertThrows(IllegalStateException.class, values::hasNext);
			assertEquals("Unknown URI ID: 2", failure.getMessage());
			assertFalse(values.hasNext());
		}
		verify(base, times(1)).close();
	}

	@Test
	public void smallLimitCapsReadAheadAndEarlyCloseDiscardsBufferedRows() {
		var base = rows(List.of(new KvinRecord(1, 1, 1, 3, 0, 3),
				new KvinRecord(1, 1, 2, 2, 0, 2),
				new KvinRecord(1, 1, 3, 1, 0, 1)), 2);
		when(ids.getUris(Set.of(1L, 2L), IdKind.PROPERTY)).thenReturn(Map.of(1L, first, 2L, second));
		var values = store.fetch(item, null, null, 2);
		assertEquals(first, values.next().property);
		verify(base, times(2)).next();
		values.close();
		assertFalse(values.hasNext());
		verify(base, times(1)).close();
		verify(ids, never()).getUris(Set.of(3L), IdKind.PROPERTY);
	}

	@Test
	public void lookupFailureClosesReaderAndPreservesCloseFailure() {
		var base = rows(List.of(new KvinRecord(1, 1, 1, 1, 0, 1)), 0);
		var failure = new IllegalStateException("lookup failed");
		var closeFailure = new IllegalStateException("close failed");
		when(ids.getUris(Set.of(1L), IdKind.PROPERTY)).thenThrow(failure);
		doThrow(closeFailure).when(base).close();
		var values = store.fetch(item, null, null, 0);
		assertSame(failure, assertThrows(IllegalStateException.class, values::hasNext));
		assertArrayEquals(new Throwable[]{closeFailure}, failure.getSuppressed());
		assertFalse(values.hasNext());
		verify(base, times(1)).close();
	}

	@Test
	public void explicitPropertiesNeedNeitherReadAheadNorReverseLookup() {
		when(ids.getIds(List.of(first), IdKind.PROPERTY, false)).thenReturn(Map.of(first, 1L));
		var base = spy(WrappedIterator.create(List.of(new KvinRecord(1, 1, 1, 2, 0, 2),
				new KvinRecord(1, 1, 1, 1, 0, 1)).iterator()));
		when(records.fetch(1L, 1L, 1L, 0)).thenReturn(base);
		try (var values = store.fetch(item, first, null, 0)) {
			assertEquals(first, values.next().property);
			verify(base, times(1)).next();
		}
		verify(ids, never()).getUris(anyCollection(), any());
	}

	@Test
	public void propertiesUseBulkInterfaceAndPreserveRecordStoreOrder() {
		when(ids.getId(item, IdKind.ITEM, false)).thenReturn(1L);
		when(records.properties(1L, 1L)).thenReturn(WrappedIterator.create(List.of(2L, 1L).iterator()));
		when(ids.getUris(List.of(2L, 1L), IdKind.PROPERTY)).thenReturn(Map.of(1L, first, 2L, second));
		try (var properties = store.properties(item, null)) {
			assertEquals(List.of(second, first), properties.toList());
		}
		verify(ids, times(1)).getUris(List.of(2L, 1L), IdKind.PROPERTY);
		verify(ids, never()).getUri(anyLong(), any());
	}
}
