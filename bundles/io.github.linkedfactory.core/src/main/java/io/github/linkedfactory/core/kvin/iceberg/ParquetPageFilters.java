package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;

import java.util.HashSet;
import java.util.Set;

/**
 * Converts the KVIN fetch filters to Parquet predicates, so that Parquet's column
 * index can skip pages within a row group.
 *
 * <p>The result may match more rows than the expression (callers still evaluate the
 * expression per row), but never fewer. Unsupported parts become "no restriction";
 * {@code null} means that the whole expression puts no restriction on pages.
 * Iceberg's own converter is package-private, so this handles the expressions that
 * {@link IcebergRecordStore} builds: and, or, and comparisons of long and int columns.</p>
 */
final class ParquetPageFilters {
	private ParquetPageFilters() {
	}

	static FilterPredicate convert(Schema schema, Expression expr) {
		switch (expr.op()) {
			case AND: {
				And and = (And) expr;
				FilterPredicate left = convert(schema, and.left());
				FilterPredicate right = convert(schema, and.right());
				if (left == null) {
					return right;
				}
				return right == null ? left : FilterApi.and(left, right);
			}
			case OR: {
				Or or = (Or) expr;
				FilterPredicate left = convert(schema, or.left());
				FilterPredicate right = convert(schema, or.right());
				return left == null || right == null ? null : FilterApi.or(left, right);
			}
			default:
				return expr instanceof UnboundPredicate<?> predicate ? convert(schema, predicate) : null;
		}
	}

	private static FilterPredicate convert(Schema schema, UnboundPredicate<?> predicate) {
		var field = schema.findField(predicate.ref().name());
		if (field == null) {
			return null;
		}
		Type.TypeID type = field.type().typeId();
		if (type == Type.TypeID.LONG) {
			return compare(FilterApi.longColumn(field.name()), predicate, Number::longValue);
		} else if (type == Type.TypeID.INTEGER) {
			return compare(FilterApi.intColumn(field.name()), predicate, Number::intValue);
		}
		return null;
	}

	private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsLtGt> FilterPredicate compare(
			C column, UnboundPredicate<?> predicate, java.util.function.Function<Number, T> value) {
		switch (predicate.op()) {
			case EQ:
				return FilterApi.eq(column, literal(predicate.literal(), value));
			case NOT_EQ:
				return FilterApi.notEq(column, literal(predicate.literal(), value));
			case LT:
				return FilterApi.lt(column, literal(predicate.literal(), value));
			case LT_EQ:
				return FilterApi.ltEq(column, literal(predicate.literal(), value));
			case GT:
				return FilterApi.gt(column, literal(predicate.literal(), value));
			case GT_EQ:
				return FilterApi.gtEq(column, literal(predicate.literal(), value));
			case IN: {
				Set<T> values = new HashSet<>();
				for (Literal<?> literal : predicate.literals()) {
					values.add(literal(literal, value));
				}
				return FilterApi.in(column, values);
			}
			default:
				return null;
		}
	}

	private static <T> T literal(Literal<?> literal, java.util.function.Function<Number, T> value) {
		return value.apply((Number) literal.value());
	}
}
