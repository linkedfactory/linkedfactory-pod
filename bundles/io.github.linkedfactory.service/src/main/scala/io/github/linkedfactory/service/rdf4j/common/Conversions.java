package io.github.linkedfactory.service.rdf4j.common;

import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.service.rdf4j.kvin.KvinEvaluationStrategy;
import net.enilink.komma.core.URI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleBNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class Conversions {
	public static class BNodeWithValue extends SimpleBNode {

		private static final String uniqueIdPrefix = UUID.randomUUID().toString().replace("-", "");
		private static final AtomicLong uniqueIdSuffix = new AtomicLong();
		public final Object value;

		public BNodeWithValue(Object value) {
			super(generateId());
			this.value = value;
		}

		static String generateId() {
			return uniqueIdPrefix + uniqueIdSuffix.incrementAndGet();
		}
	}

	public static long getLongValue(Value v, long defaultValue) {
		if (v instanceof Literal) {
			return ((Literal) v).longValue();
		}
		return defaultValue;
	}

	public static Value toRdfValue(Object value, ValueFactory vf) {
		Value rdfValue;
		if (value instanceof URI) {
			String valueStr = ((URI) value).isRelative() ? "r:" + value : value.toString();
			rdfValue = vf.createIRI(valueStr);
		} else if (value instanceof Double) {
			rdfValue = vf.createLiteral((Double) value);
		} else if (value instanceof Float) {
			rdfValue = vf.createLiteral((Float) value);
		} else if (value instanceof Integer) {
			rdfValue = vf.createLiteral((Integer) value);
		} else if (value instanceof Long) {
			rdfValue = vf.createLiteral((Long) value);
		} else if (value instanceof BigDecimal) {
			rdfValue = vf.createLiteral((BigDecimal) value);
		} else if (value instanceof BigInteger) {
			rdfValue = vf.createLiteral((BigInteger) value);
		} else if (value instanceof Record) {
			return new BNodeWithValue(value);
		} else if (value instanceof Object[] || value instanceof List<?>) {
			return new BNodeWithValue(value);
		} else {
			rdfValue = vf.createLiteral(value.toString());
		}
		return rdfValue;
	}
}
