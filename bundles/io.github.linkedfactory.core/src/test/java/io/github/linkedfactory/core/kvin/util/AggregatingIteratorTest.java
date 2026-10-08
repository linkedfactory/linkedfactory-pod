package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.util.ValueUtils;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class AggregatingIteratorTest {
	private static final URI ITEM = URIs.createURI("urn:item");
	private static final URI OTHER_ITEM = URIs.createURI("urn:other-item");
	private static final URI PROPERTY = URIs.createURI("urn:property");
	private static final URI OTHER_PROPERTY = URIs.createURI("urn:other-property");
	private static final URI CONTEXT = URIs.createURI("urn:context");
	private static final URI OTHER_CONTEXT = URIs.createURI("urn:other-context");

	@Test
	public void aggregatesEveryOperatorWithValueUtilsArithmetic() {
		ValueUtils utils = ValueUtils.getInstance();
		List<KvinTuple> values = List.of(tuple(29, 7), tuple(28, 2L), tuple(21, 4.5));
		Object sum = utils.add(utils.add(7, 2L), 4.5);
		assertAggregate(values, "first", 7);
		assertAggregate(values, "min", 2L);
		assertAggregate(values, "max", 7);
		assertAggregate(values, "sum", sum);
		assertAggregate(values, "avg", utils.divide(sum, 3L));
	}

	@Test
	public void preservesDecimalAndBigIntegerArithmetic() {
		for (List<KvinTuple> values : List.of(
				List.of(tuple(29, new BigDecimal("1.25")), tuple(21, new BigDecimal("2.50"))),
				List.of(tuple(29, new BigInteger("123456789012345678901234567890")), tuple(21, BigInteger.ONE)))) {
			Object sum = ValueUtils.getInstance().add(values.get(0).value, values.get(1).value);
			assertAggregate(values, "sum", sum);
			assertAggregate(values, "avg", ValueUtils.getInstance().divide(sum, 2L));
		}
	}

	@Test
	public void preservesComparisonConversionAndFirstFallback() {
		List<KvinTuple> values = List.of(tuple(29, "9"), tuple(21, 3));
		assertAggregate(values, "min", 3);
		assertAggregate(values, "max", "9");
		assertAggregate(values, "first", "9");
		assertAggregate(values, "unknown", "9");
	}

	@Test
	public void preservesFirstTupleMetadataAndContextSemantics() {
		KvinTuple first = new KvinTuple(ITEM, PROPERTY, CONTEXT, 29, 42, 7);
		KvinTuple second = new KvinTuple(ITEM, PROPERTY, OTHER_CONTEXT, 21, 99, 3);
		KvinTuple result = iterator(List.of(first, second), 10, "sum", 0).next();
		assertSame(first.item, result.item);
		assertSame(first.property, result.property);
		assertSame(first.context, result.context);
		assertEquals(20, result.time);
		assertEquals(1, result.seqNr);
		assertEquals(ValueUtils.getInstance().add(7, 3), result.value);
	}

	@Test
	public void includesFinalLookaheadWhenBaseIsExhausted() {
		AggregatingIterator<KvinTuple> iterator = iterator(List.of(tuple(20, 7), tuple(19, 3)), 10, "sum", 0);
		assertTrue(iterator.hasNext());
		assertTrue(iterator.hasNext());
		assertEquals(20, iterator.next().time);
		assertTrue(iterator.hasNext());
		KvinTuple last = iterator.next();
		assertEquals(10, last.time);
		assertEquals(3, last.value);
		assertEquals(2, last.seqNr);
		assertFalse(iterator.hasNext());
		assertThrows(NoSuchElementException.class, iterator::next);
	}

	@Test
	public void separatesItemAndPropertySwitchesEvenInSameInterval() {
		List<KvinTuple> values = List.of(tuple(29, 1),
				new KvinTuple(OTHER_ITEM, PROPERTY, CONTEXT, 28, 2),
				new KvinTuple(OTHER_ITEM, OTHER_PROPERTY, CONTEXT, 27, 3));
		List<KvinTuple> results = iterator(values, 10, "sum", 0).toList();
		assertEquals(3, results.size());
		for (int i = 0; i < results.size(); i++) {
			assertEquals(values.get(i).item, results.get(i).item);
			assertEquals(values.get(i).property, results.get(i).property);
			assertEquals(i + 1, results.get(i).value);
			assertEquals(i + 1, results.get(i).seqNr);
		}
	}

	@Test
	public void equalUrisDoNotSplitSeries() {
		List<KvinTuple> values = List.of(tuple(29, 1),
				new KvinTuple(URIs.createURI(ITEM.toString()), URIs.createURI(PROPERTY.toString()), CONTEXT, 21, 2));
		assertAggregate(values, "sum", ValueUtils.getInstance().add(1, 2));
	}

	@Test
	public void zeroIntervalAggregatesWholeSeries() {
		List<KvinTuple> results = iterator(List.of(tuple(29, 1), tuple(0, 2),
				new KvinTuple(OTHER_ITEM, PROPERTY, CONTEXT, 17, 3)), 0, "sum", 0).toList();
		assertEquals(2, results.size());
		assertEquals(0, results.get(0).time);
		assertEquals(0, results.get(1).time);
		assertEquals(ValueUtils.getInstance().add(1, 2), results.get(0).value);
	}

	@Test
	public void aggregateLimitResetsAtSeriesBoundaryNotBeforePreviousResult() {
		List<KvinTuple> values = List.of(tuple(39, 1), tuple(29, 2), tuple(28, 3), tuple(19, 4),
				new KvinTuple(OTHER_ITEM, PROPERTY, CONTEXT, 39, 5),
				new KvinTuple(OTHER_ITEM, PROPERTY, CONTEXT, 29, 6),
				new KvinTuple(OTHER_ITEM, OTHER_PROPERTY, CONTEXT, 29, 7));
		List<KvinTuple> results = iterator(values, 10, "sum", 2).toList();
		assertEquals(5, results.size());
		assertEquals(List.of(30L, 20L, 30L, 20L, 20L),
				results.stream().map(tuple -> tuple.time).toList());
		assertEquals(7, results.get(4).value);
		assertEquals(5, results.get(4).seqNr);
	}

	@Test
	public void skipsLimitedSeriesAndStillIncludesFinalSeriesLookahead() {
		List<KvinTuple> values = List.of(tuple(39, 1), tuple(29, 2), tuple(19, 3),
				new KvinTuple(OTHER_ITEM, PROPERTY, CONTEXT, 19, 4));
		List<KvinTuple> results = iterator(values, 10, "first", 1).toList();
		assertEquals(2, results.size());
		assertEquals(1, results.get(0).value);
		assertEquals(4, results.get(1).value);
	}

	@Test
	public void invalidNumbersYieldZeroAndDrainOnlyCurrentInterval() {
		for (String op : List.of("min", "max", "avg")) {
			List<KvinTuple> values = List.of(tuple(29, "not-a-number"), tuple(28, 1),
					tuple(21, 2), tuple(19, 5));
			List<KvinTuple> results = iterator(values, 10, op, 0).toList();
			assertEquals(2, results.size());
			assertEquals(0, results.get(0).value);
			Object expected = "avg".equals(op) ? ValueUtils.getInstance().divide(5, 1L) : 5;
			assertEquals(expected, results.get(1).value);
		}
		assertAggregate(List.of(tuple(29, "not-a-number")), "avg", 0);
	}

	@Test
	public void accumulatesBeforeConsumingEntireInterval() {
		AtomicBoolean added = new AtomicBoolean();
		Object first = new Object() {
			@Override
			public String toString() {
				added.set(true);
				return "1";
			}
		};
		Iterator<KvinTuple> source = new Iterator<>() {
			int index;

			@Override
			public boolean hasNext() {
				return index < 10000;
			}

			@Override
			public KvinTuple next() {
				if (index > 1) {
					assertTrue("arithmetic must run before the interval is fully read", added.get());
				}
				return tuple(29, index++ == 0 ? first : "1");
			}
		};
		assertEquals("1".repeat(10000), iterator(source, 10, "sum", 0).next().value);
	}

	@Test
	public void emptyInputAndCloseRemainSupported() {
		AtomicBoolean closed = new AtomicBoolean();
		NiceIterator<KvinTuple> source = new NiceIterator<>() {
			@Override
			public void close() {
				closed.set(true);
			}
		};
		AggregatingIterator<KvinTuple> iterator = iterator(source, 10, "sum", 0);
		assertFalse(iterator.hasNext());
		assertThrows(NoSuchElementException.class, iterator::next);
		iterator.close();
		assertTrue(closed.get());
	}

	@Test
	public void sourceExceptionsAreNotConvertedToInvalidNumbers() {
		Iterator<KvinTuple> source = new Iterator<>() {
			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public KvinTuple next() {
				throw new NumberFormatException("source failure");
			}
		};
		assertThrows(NumberFormatException.class, iterator(source, 10, "sum", 0)::hasNext);
	}

	@Test
	public void preservesInheritedProtectedAggregateOverrides() {
		class CustomAggregation extends AggregatingIterator<KvinTuple> {
			CustomAggregation() {
				super(List.of(tuple(29, 1), tuple(21, 2), tuple(19, 3)).iterator(), 10, "sum", 0);
			}

			@Override
			protected Object aggregate(List<KvinTuple> elements, String op) {
				assertEquals("sum", op);
				return List.of(elements.size(), super.aggregate(elements, op));
			}

			@Override
			protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
				return new KvinTuple(item, property, context, time, seqNr, value);
			}
		}
		List<KvinTuple> results = new CustomAggregation() {}.toList();
		assertEquals(2, results.size());
		assertEquals(List.of(2, ValueUtils.getInstance().add(1, 2)), results.get(0).value);
		assertEquals(List.of(1, 3), results.get(1).value);
	}

	@Test
	public void invalidNumbersFromProtectedAggregateOverridesStillYieldZero() {
		AggregatingIterator<KvinTuple> iterator = new AggregatingIterator<>(
				List.of(tuple(29, 1), tuple(21, 2), tuple(19, 3)).iterator(), 10, "custom", 0) {
			@Override
			protected Object aggregate(List<KvinTuple> elements, String op) {
				if (elements.size() > 1) {
					throw new NumberFormatException("custom aggregation failure");
				}
				return elements.get(0).value;
			}

			@Override
			protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
				return new KvinTuple(item, property, context, time, seqNr, value);
			}
		};
		List<KvinTuple> results = iterator.toList();
		assertEquals(2, results.size());
		assertEquals(0, results.get(0).value);
		assertEquals(3, results.get(1).value);
	}

	private static void assertAggregate(List<KvinTuple> values, String op, Object expected) {
		List<KvinTuple> results = iterator(values, 10, op, 0).toList();
		assertEquals(1, results.size());
		assertEquals(expected, results.get(0).value);
	}

	private static KvinTuple tuple(long time, Object value) {
		return new KvinTuple(ITEM, PROPERTY, CONTEXT, time, value);
	}

	private static AggregatingIterator<KvinTuple> iterator(List<KvinTuple> values, long interval, String op, long limit) {
		return iterator(values.iterator(), interval, op, limit);
	}

	private static AggregatingIterator<KvinTuple> iterator(Iterator<KvinTuple> source, long interval, String op, long limit) {
		return new AggregatingIterator<>(source, interval, op, limit) {
			@Override
			protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
				return new KvinTuple(item, property, context, time, seqNr, value);
			}
		};
	}
}
