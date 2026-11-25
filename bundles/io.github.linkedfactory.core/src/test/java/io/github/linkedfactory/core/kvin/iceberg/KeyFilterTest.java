package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class KeyFilterTest {
	private final Schema schema = new Schema(
			Types.NestedField.required(1, "itemId", Types.LongType.get()),
			Types.NestedField.required(2, "contextId", Types.LongType.get()),
			Types.NestedField.required(3, "propertyId", Types.LongType.get()),
			Types.NestedField.required(4, "time", Types.LongType.get()),
			Types.NestedField.required(5, "seqNr", Types.IntegerType.get()),
			Types.NestedField.required(6, "first", Types.BooleanType.get()),
			Types.NestedField.optional(7, "valueString", Types.StringType.get()));

	private List<Expression> compiledExpressions() {
		List<Long> items = new ArrayList<>();
		for (long i = 1; i <= 12; i++) {
			// more than 8 values use the binary search
			items.add(i * 3);
		}
		return List.of(
				Expressions.alwaysTrue(),
				Expressions.alwaysFalse(),
				Expressions.and(Expressions.in("itemId", items), Expressions.equal("contextId", 1L)),
				Expressions.and(Expressions.in("itemId", 2L, 4L), Expressions.in("propertyId", 1L, 3L)),
				Expressions.or(Expressions.lessThan("time", 10L), Expressions.greaterThanOrEqual("time", 30L)),
				Expressions.and(Expressions.lessThanOrEqual("time", 20L), Expressions.greaterThan("seqNr", 2)),
				Expressions.not(Expressions.notEqual("propertyId", 2L)),
				Expressions.notIn("itemId", 1L, 2L, 3L),
				Expressions.or(Expressions.greaterThan("itemId", 5L), Expressions.and(Expressions.equal("itemId", 5L),
						Expressions.or(Expressions.greaterThan("contextId", 1L), Expressions.and(Expressions.equal("contextId", 1L),
								Expressions.greaterThan("propertyId", 2L))))),
				Expressions.and(Expressions.notNull("itemId"), Expressions.not(Expressions.isNull("time"))));
	}

	@Test
	public void compiledFiltersMatchIcebergEvaluator() {
		Random random = new Random(42);
		GenericRecord row = GenericRecord.create(schema);
		for (Expression expr : compiledExpressions()) {
			KeyFilter filter = KeyFilter.of(schema, expr);
			assertNotEquals("expected a compiled filter for " + expr, "Fallback", filter.getClass().getSimpleName());
			Evaluator evaluator = new Evaluator(schema.asStruct(), expr);
			for (int i = 0; i < 2000; i++) {
				long itemId = random.nextInt(40), contextId = random.nextInt(3), propertyId = random.nextInt(5);
				long time = random.nextInt(40);
				int seqNr = random.nextInt(5);
				row.set(0, itemId);
				row.set(1, contextId);
				row.set(2, propertyId);
				row.set(3, time);
				row.set(4, seqNr);
				assertEquals(expr + " on " + row, evaluator.eval(row),
						filter.matches(itemId, contextId, propertyId, time, seqNr));
			}
		}
	}

	@Test
	public void otherColumnsUseTheEvaluatorOnKeyFields() {
		Expression expr = Expressions.and(Expressions.equal("itemId", 1L), Expressions.equal("first", true));
		KeyFilter filter = KeyFilter.of(schema, expr);
		assertEquals("Fallback", filter.getClass().getSimpleName());
		// like the evaluator on a record with only the key fields: first is null
		assertFalse(filter.matches(1, 0, 0, 0, 0));
		KeyFilter onKeys = KeyFilter.of(schema, Expressions.or(Expressions.equal("itemId", 1L),
				Expressions.isNull("valueString")));
		assertEquals("Fallback", onKeys.getClass().getSimpleName());
		assertTrue(onKeys.matches(2, 0, 0, 0, 0));
	}
}
