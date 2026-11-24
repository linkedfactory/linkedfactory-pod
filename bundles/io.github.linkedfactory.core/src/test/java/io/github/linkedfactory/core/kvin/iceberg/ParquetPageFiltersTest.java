package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class ParquetPageFiltersTest {
	private final Schema schema = new Schema(
			Types.NestedField.required(1, "itemId", Types.LongType.get()),
			Types.NestedField.required(2, "seqNr", Types.IntegerType.get()),
			Types.NestedField.optional(3, "valueString", Types.StringType.get()));

	@Test
	public void convertsComparisonsOfLongAndIntColumns() {
		var expr = Expressions.and(Expressions.in("itemId", List.of(1L, 2L)),
				Expressions.and(Expressions.greaterThanOrEqual("seqNr", 3), Expressions.lessThan("itemId", 9L)));
		var expected = FilterApi.and(FilterApi.in(FilterApi.longColumn("itemId"), Set.of(1L, 2L)),
				FilterApi.and(FilterApi.gtEq(FilterApi.intColumn("seqNr"), 3), FilterApi.lt(FilterApi.longColumn("itemId"), 9L)));
		assertEquals(expected, ParquetPageFilters.convert(schema, expr));
	}

	@Test
	public void dropsUnsupportedConjunctsButNotUnsupportedAlternatives() {
		var unsupported = Expressions.equal("valueString", "x");
		var supported = Expressions.equal("itemId", 1L);
		assertEquals(FilterApi.eq(FilterApi.longColumn("itemId"), 1L),
				ParquetPageFilters.convert(schema, Expressions.and(unsupported, supported)));
		assertNull(ParquetPageFilters.convert(schema, Expressions.or(unsupported, supported)));
		assertNull(ParquetPageFilters.convert(schema, Expressions.alwaysTrue()));
		assertEquals(FilterApi.or(FilterApi.gt(FilterApi.longColumn("itemId"), 1L), FilterApi.eq(FilterApi.longColumn("itemId"), 1L)),
				ParquetPageFilters.convert(schema, Expressions.or(Expressions.greaterThan("itemId", 1L), supported)));
	}
}
