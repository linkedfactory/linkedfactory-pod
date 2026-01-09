package io.github.linkedfactory.core.rdf4j.common;

import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.util.JsonFormatWriter;
import io.github.linkedfactory.core.rdf4j.kvin.KvinEvaluationStrategy;
import net.enilink.komma.core.URI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Conversions {
	static class InternalJsonFormatWriter extends JsonFormatWriter {
		InternalJsonFormatWriter(OutputStream outputStream) throws IOException {
			super(outputStream);
		}

		@Override
		protected void initialStartObject() {
			// do nothing
		}

		@Override
		protected void writeValue(Object value) throws IOException {
			super.writeValue(value);
		}

		@Override
		public void end() throws IOException {
			// do nothing
		}
	}

	public static long getLongValue(Value v, long defaultValue) {
		if (v instanceof Literal) {
			return ((Literal) v).longValue();
		}
		return defaultValue;
	}

	public static Value toJsonRdfValue(Object value, ValueFactory vf) throws QueryEvaluationException {
		if (value instanceof Record || value instanceof Object[] || value instanceof URI) {
			var baos = new ByteArrayOutputStream();
			try {
				var writer = new InternalJsonFormatWriter(baos);
				writer.writeValue(value);
				writer.close();
			} catch (Exception e) {
				throw new QueryEvaluationException(e);
			}
			return vf.createLiteral(baos.toString(StandardCharsets.UTF_8));
		} else {
			return Conversions.toRdfValue(value, vf);
		}
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
			var uri = ((Record) value).first(IRIWithValue.PROPERTY).getValue();
			return uri != null ? IRIWithValue.create(uri.toString(), value) : BNodeWithValue.create(value, useCache);
		} else if (value instanceof Object[] || value instanceof List<?>) {
			return BNodeWithValue.create(value, useCache);
		} else {
			rdfValue = vf.createLiteral(value.toString());
		}
		return rdfValue;
	}
}
