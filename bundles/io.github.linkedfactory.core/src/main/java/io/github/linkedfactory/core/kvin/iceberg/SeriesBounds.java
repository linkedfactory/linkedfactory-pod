package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;

import java.util.Arrays;
import java.util.List;

/**
 * The series that a fetch filter allows: one context, a set of items, and optionally a set
 * of properties, taken from equality and in predicates that the filter requires.
 *
 * <p>In a file whose rows are sorted by context, item and property, the rows of the next
 * allowed series can then be searched instead of looking at each row in between.</p>
 */
final class SeriesBounds {
	private final long context;
	private final long[] items;
	// null if any property is allowed
	private final long[] properties;

	private SeriesBounds(long context, long[] items, long[] properties) {
		this.context = context;
		this.items = items;
		this.properties = properties;
	}

	/**
	 * Returns the series that a filter allows, or {@code null} if it does not require one
	 * context and a set of items.
	 */
	static SeriesBounds of(Expression filter) {
		Long[] context = new Long[1];
		long[][] items = new long[1][];
		long[][] properties = new long[1][];
		collect(filter, context, items, properties);
		return context[0] == null || items[0] == null ? null : new SeriesBounds(context[0], items[0], properties[0]);
	}

	/**
	 * Collects the equality and in predicates of the conjuncts of a filter. Only conjuncts
	 * restrict the allowed series, so or and not are ignored.
	 */
	private static void collect(Expression filter, Long[] context, long[][] items, long[][] properties) {
		if (filter instanceof And and) {
			collect(and.left(), context, items, properties);
			collect(and.right(), context, items, properties);
		} else if (filter instanceof UnboundPredicate<?> predicate) {
			long[] values = switch (predicate.op()) {
				case EQ -> values(List.of(predicate.literal()));
				case IN -> values(predicate.literals());
				default -> null;
			};
			if (values == null) {
				return;
			}
			switch (predicate.ref().name()) {
				case "contextId" -> {
					// several equalities of the context: keep one, the key filter checks all
					if (context[0] == null && values.length == 1) {
						context[0] = values[0];
					}
				}
				case "itemId" -> items[0] = items[0] == null ? values : intersect(items[0], values);
				case "propertyId" -> properties[0] = properties[0] == null ? values : intersect(properties[0], values);
				default -> {
				}
			}
		}
	}

	private static long[] values(List<? extends Literal<?>> literals) {
		long[] values = new long[literals.size()];
		for (int i = 0; i < values.length; i++) {
			if (!(literals.get(i).value() instanceof Number number)) {
				return null;
			}
			values[i] = number.longValue();
		}
		Arrays.sort(values);
		return values;
	}

	private static long[] intersect(long[] a, long[] b) {
		return Arrays.stream(a).filter(value -> Arrays.binarySearch(b, value) >= 0).toArray();
	}

	/**
	 * Returns the smallest allowed series, as context, item and property, that is not before
	 * the given one, or {@code null} if there is none. A property of {@link Long#MIN_VALUE}
	 * stands for the first property of an item.
	 */
	long[] next(long context, long item, long property) {
		if (context > this.context) {
			return null;
		}
		if (context < this.context) {
			return first(0);
		}
		int i = lowerBound(items, item);
		if (i == items.length) {
			return null;
		}
		if (items[i] > item) {
			return first(i);
		}
		if (properties == null) {
			return new long[]{context, item, property};
		}
		int p = lowerBound(properties, property);
		if (p < properties.length) {
			return new long[]{context, item, properties[p]};
		}
		return i + 1 < items.length ? first(i + 1) : null;
	}

	private long[] first(int item) {
		if (item >= items.length || properties != null && properties.length == 0) {
			return null;
		}
		return new long[]{context, items[item], properties == null ? Long.MIN_VALUE : properties[0]};
	}

	private static int lowerBound(long[] values, long value) {
		int i = Arrays.binarySearch(values, value);
		if (i < 0) {
			return -i - 1;
		}
		// the first of equal values
		while (i > 0 && values[i - 1] == value) {
			i--;
		}
		return i;
	}
}
