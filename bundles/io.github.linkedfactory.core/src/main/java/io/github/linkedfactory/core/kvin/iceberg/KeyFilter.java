package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;

import java.util.Arrays;
import java.util.Collection;

/**
 * Evaluates an Iceberg filter on the primitive key fields of a row (itemId,
 * contextId, propertyId, time, seqNr) without boxing them.
 *
 * <p>Supported are and, or, not, true, false, comparisons, in, not in, and null
 * checks on the key columns, which covers the filters that {@link IcebergRecordStore}
 * builds. Any other expression is evaluated with Iceberg's {@link Evaluator} on a
 * record that holds only the key fields, as before.</p>
 */
abstract class KeyFilter {
	static final String[] KEY_COLUMNS = {"itemId", "contextId", "propertyId", "time", "seqNr"};

	abstract boolean matches(long itemId, long contextId, long propertyId, long time, int seqNr);

	static KeyFilter of(Schema schema, Expression expr) {
		KeyFilter compiled = compile(expr);
		return compiled != null ? compiled : new Fallback(schema, expr);
	}

	/**
	 * @return the compiled filter, or {@code null} if the expression contains
	 * anything that is not supported
	 */
	private static KeyFilter compile(Expression expr) {
		switch (expr.op()) {
			case TRUE:
				return Constant.TRUE;
			case FALSE:
				return Constant.FALSE;
			case AND: {
				KeyFilter left = compile(((And) expr).left());
				KeyFilter right = left == null ? null : compile(((And) expr).right());
				return right == null ? null : new AndFilter(left, right);
			}
			case OR: {
				KeyFilter left = compile(((Or) expr).left());
				KeyFilter right = left == null ? null : compile(((Or) expr).right());
				return right == null ? null : new OrFilter(left, right);
			}
			case NOT: {
				KeyFilter child = compile(((Not) expr).child());
				return child == null ? null : new NotFilter(child);
			}
			default:
				return expr instanceof UnboundPredicate<?> predicate ? compile(predicate) : null;
		}
	}

	private static KeyFilter compile(UnboundPredicate<?> predicate) {
		int column = Arrays.asList(KEY_COLUMNS).indexOf(predicate.ref().name());
		if (column < 0) {
			return null;
		}
		switch (predicate.op()) {
			case IS_NULL:
				// key columns are required
				return Constant.FALSE;
			case NOT_NULL:
				return Constant.TRUE;
			case EQ:
			case NOT_EQ:
			case LT:
			case LT_EQ:
			case GT:
			case GT_EQ: {
				Long value = value(predicate.literal());
				return value == null ? null : new Compare(column, predicate.op(), value);
			}
			case IN:
			case NOT_IN: {
				long[] values = values(predicate.literals());
				if (values == null) {
					return null;
				}
				KeyFilter in = new In(column, values);
				return predicate.op() == Expression.Operation.IN ? in : new NotFilter(in);
			}
			default:
				return null;
		}
	}

	private static Long value(Literal<?> literal) {
		Object value = literal == null ? null : literal.value();
		return value instanceof Long || value instanceof Integer ? ((Number) value).longValue() : null;
	}

	private static long[] values(Collection<? extends Literal<?>> literals) {
		long[] values = new long[literals.size()];
		int i = 0;
		for (Literal<?> literal : literals) {
			Long value = value(literal);
			if (value == null) {
				return null;
			}
			values[i++] = value;
		}
		Arrays.sort(values);
		return values;
	}

	static long key(int column, long itemId, long contextId, long propertyId, long time, int seqNr) {
		return switch (column) {
			case 0 -> itemId;
			case 1 -> contextId;
			case 2 -> propertyId;
			case 3 -> time;
			default -> seqNr;
		};
	}

	private static final class Constant extends KeyFilter {
		static final Constant TRUE = new Constant(true);
		static final Constant FALSE = new Constant(false);
		final boolean result;

		Constant(boolean result) {
			this.result = result;
		}

		@Override
		boolean matches(long itemId, long contextId, long propertyId, long time, int seqNr) {
			return result;
		}
	}

	private static final class AndFilter extends KeyFilter {
		final KeyFilter left, right;

		AndFilter(KeyFilter left, KeyFilter right) {
			this.left = left;
			this.right = right;
		}

		@Override
		boolean matches(long itemId, long contextId, long propertyId, long time, int seqNr) {
			return left.matches(itemId, contextId, propertyId, time, seqNr)
					&& right.matches(itemId, contextId, propertyId, time, seqNr);
		}
	}

	private static final class OrFilter extends KeyFilter {
		final KeyFilter left, right;

		OrFilter(KeyFilter left, KeyFilter right) {
			this.left = left;
			this.right = right;
		}

		@Override
		boolean matches(long itemId, long contextId, long propertyId, long time, int seqNr) {
			return left.matches(itemId, contextId, propertyId, time, seqNr)
					|| right.matches(itemId, contextId, propertyId, time, seqNr);
		}
	}

	private static final class NotFilter extends KeyFilter {
		final KeyFilter child;

		NotFilter(KeyFilter child) {
			this.child = child;
		}

		@Override
		boolean matches(long itemId, long contextId, long propertyId, long time, int seqNr) {
			return !child.matches(itemId, contextId, propertyId, time, seqNr);
		}
	}

	private static final class Compare extends KeyFilter {
		final int column;
		final Expression.Operation op;
		final long value;

		Compare(int column, Expression.Operation op, long value) {
			this.column = column;
			this.op = op;
			this.value = value;
		}

		@Override
		boolean matches(long itemId, long contextId, long propertyId, long time, int seqNr) {
			long key = key(column, itemId, contextId, propertyId, time, seqNr);
			return switch (op) {
				case EQ -> key == value;
				case NOT_EQ -> key != value;
				case LT -> key < value;
				case LT_EQ -> key <= value;
				case GT -> key > value;
				case GT_EQ -> key >= value;
				default -> throw new IllegalStateException("Unexpected operation: " + op);
			};
		}
	}

	private static final class In extends KeyFilter {
		final int column;
		final long[] values;

		In(int column, long[] values) {
			this.column = column;
			this.values = values;
		}

		@Override
		boolean matches(long itemId, long contextId, long propertyId, long time, int seqNr) {
			long key = key(column, itemId, contextId, propertyId, time, seqNr);
			if (values.length <= 8) {
				for (long value : values) {
					if (value == key) {
						return true;
					}
				}
				return false;
			}
			return Arrays.binarySearch(values, key) >= 0;
		}
	}

	/**
	 * Iceberg's evaluator on the key fields, for expressions that are not compiled.
	 */
	private static final class Fallback extends KeyFilter {
		final Evaluator evaluator;
		final GenericRecord keys;

		Fallback(Schema schema, Expression expr) {
			this.evaluator = new Evaluator(schema.asStruct(), expr);
			this.keys = GenericRecord.create(schema);
		}

		@Override
		boolean matches(long itemId, long contextId, long propertyId, long time, int seqNr) {
			keys.set(0, itemId);
			keys.set(1, contextId);
			keys.set(2, propertyId);
			keys.set(3, time);
			keys.set(4, seqNr);
			return evaluator.eval(keys);
		}
	}
}
