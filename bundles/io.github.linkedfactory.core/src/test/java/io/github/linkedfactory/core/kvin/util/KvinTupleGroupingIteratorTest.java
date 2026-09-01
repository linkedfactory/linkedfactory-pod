package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class KvinTupleGroupingIteratorTest {
	private static final URI ITEM_A = URIs.createURI("urn:item:a");
	private static final URI ITEM_B = URIs.createURI("urn:item:b");
	private static final URI PROPERTY = URIs.createURI("urn:property");
	private static final URI PROPERTY_B = URIs.createURI("urn:property:b");
	private static final URI CONTEXT = URIs.createURI("urn:context");
	private static final URI CONTEXT_B = URIs.createURI("urn:context:b");

	@Test
	public void groupsByIdentityWithinBoundedWindows() {
		KvinTuple a1 = tuple(ITEM_A, 1, 1);
		KvinTuple b1 = tuple(ITEM_B, 2, 1);
		KvinTuple a2 = tuple(ITEM_A, 3, 2);
		KvinTuple b2 = tuple(ITEM_B, 4, 2);
		KvinTuple a3 = tuple(ITEM_A, 5, 3);

		IExtendedIterator<KvinTuple> grouped = new KvinTupleGroupingIterator(
				WrappedIterator.create(List.of(a1, b1, a2, b2, a3).iterator()), 4);
		List<KvinTuple> actual = new ArrayList<>();
		grouped.forEachRemaining(actual::add);

		assertEquals(List.of(a1, a2, b1, b2, a3), actual);
		assertFalse(grouped.hasNext());
	}

	@Test
	public void closesSourceOnFailureAndExplicitClose() {
		TrackingIterator source = new TrackingIterator(List.of(tuple(ITEM_A, 1, 1)));
		KvinTupleGroupingIterator grouped = new KvinTupleGroupingIterator(source, 1);
		assertTrue(grouped.hasNext());
		assertSame(source.values.get(0), grouped.next());
		assertFalse(grouped.hasNext());
		assertTrue(source.closed);
		grouped.close();
		assertTrue(source.closed);
	}

	@Test
	public void distinguishesPropertyAndContextInSeriesIdentity() {
		KvinTuple first = new KvinTuple(ITEM_A, PROPERTY, CONTEXT, 1, 1, "first");
		KvinTuple otherProperty = new KvinTuple(ITEM_A, PROPERTY_B, CONTEXT, 2, 1, "property");
		KvinTuple otherContext = new KvinTuple(ITEM_A, PROPERTY, CONTEXT_B, 3, 1, "context");
		KvinTuple second = new KvinTuple(ITEM_A, PROPERTY, CONTEXT, 4, 2, "second");

		IExtendedIterator<KvinTuple> grouped = new KvinTupleGroupingIterator(
				WrappedIterator.create(List.of(first, otherProperty, otherContext, second).iterator()), 4);
		assertEquals(List.of(first, second, otherProperty, otherContext), grouped.toList());
	}

	@Test
	public void closesSourceWhenSourceFails() {
		TrackingIterator source = new TrackingIterator(List.of(tuple(ITEM_A, 1, 1)));
		source.failure = new IllegalStateException("source failed");
		KvinTupleGroupingIterator grouped = new KvinTupleGroupingIterator(source, 2);
		try {
			grouped.hasNext();
			throw new AssertionError("expected source failure");
		} catch (IllegalStateException expected) {
			assertTrue(source.closed);
		}
	}

	@Test
	public void doesNotMaskSourceFailureWhenCloseFails() {
		TrackingIterator source = new TrackingIterator(List.of(tuple(ITEM_A, 1, 1)));
		IllegalStateException sourceFailure = new IllegalStateException("source failed");
		RuntimeException closeFailure = new RuntimeException("close failed");
		source.failure = sourceFailure;
		source.closeFailure = closeFailure;
		KvinTupleGroupingIterator grouped = new KvinTupleGroupingIterator(source, 2);
		try {
			grouped.hasNext();
			throw new AssertionError("expected source failure");
		} catch (IllegalStateException actual) {
			assertSame(sourceFailure, actual);
			assertEquals(List.of(closeFailure), List.of(actual.getSuppressed()));
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNonPositiveWindow() {
		new KvinTupleGroupingIterator(WrappedIterator.create(List.<KvinTuple>of().iterator()), 0);
	}

	private static KvinTuple tuple(URI item, long time, int seqNr) {
		return new KvinTuple(item, PROPERTY, CONTEXT, time, seqNr, time);
	}

	private static final class TrackingIterator extends NiceIterator<KvinTuple> {
		private final List<KvinTuple> values;
		private int index;
		private boolean closed;
		private RuntimeException failure;
		private RuntimeException closeFailure;

		private TrackingIterator(List<KvinTuple> values) {
			this.values = values;
		}

		@Override
		public boolean hasNext() {
			if (failure != null && index >= values.size()) {
				throw failure;
			}
			return index < values.size();
		}

		@Override
		public KvinTuple next() {
			return values.get(index++);
		}

		@Override
		public void close() {
			closed = true;
			if (closeFailure != null) {
				throw closeFailure;
			}
		}
	}
}
