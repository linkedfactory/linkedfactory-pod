package io.github.linkedfactory.core.rdf4j.common;

import io.github.linkedfactory.core.kvin.Record;
import net.enilink.komma.core.URI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class Conversions {

	public static long getLongValue(Value v, long defaultValue) {
		if (v instanceof Literal) {
			return ((Literal) v).longValue();
		}
		return defaultValue;
	}

	public static Value toRdfValue(Object value, ValueFactory vf) {
		return toRdfValue(value, vf, false);
	}

	public static Value toRdfValue(Object value, ValueFactory vf, boolean useCache) {
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
			return BNodeWithValue.create(value, useCache);
		} else if (value instanceof Object[] || value instanceof List<?>) {
			return BNodeWithValue.create(value, useCache);
		} else {
			rdfValue = vf.createLiteral(value.toString());
		}
		return rdfValue;
	}
}
