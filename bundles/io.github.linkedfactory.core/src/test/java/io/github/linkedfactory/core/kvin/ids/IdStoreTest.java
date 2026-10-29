package io.github.linkedfactory.core.kvin.ids;

import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class IdStoreTest {
	@Test
	public void defaultBulkMethodsDeduplicateAndOmitMissingUris() {
		IdStore store = mock(IdStore.class, CALLS_REAL_METHODS);
		URI known = URIs.createURI("urn:ids:known");
		URI missing = URIs.createURI("urn:ids:missing");
		when(store.getId(known, IdKind.ITEM, false)).thenReturn(1L);
		when(store.getUri(1, IdKind.ITEM)).thenReturn(known);

		assertEquals(Map.of(known, 1L), store.getIds(List.of(known, missing, known, missing), IdKind.ITEM, false));
		verify(store, times(1)).getId(known, IdKind.ITEM, false);
		verify(store, times(1)).getId(missing, IdKind.ITEM, false);
		when(store.getId(missing, IdKind.ITEM, true)).thenReturn(2L);
		assertEquals(Map.of(missing, 2L), store.getIds(List.of(missing, missing), IdKind.ITEM, true));
		verify(store, times(1)).getId(missing, IdKind.ITEM, true);
		assertEquals(Map.of(1L, known), store.getUris(List.of(1L, 1L), IdKind.ITEM));
		verify(store, times(1)).getUri(1, IdKind.ITEM);
		assertThrows(IllegalStateException.class, () -> store.getUris(List.of(2L), IdKind.ITEM));
		assertTrue(store.getIds(List.of(), IdKind.ITEM, true).isEmpty());
		assertTrue(store.getUris(List.of(), IdKind.ITEM).isEmpty());
	}
}
