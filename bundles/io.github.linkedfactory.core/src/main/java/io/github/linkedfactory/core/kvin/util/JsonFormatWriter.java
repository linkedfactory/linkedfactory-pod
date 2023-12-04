package io.github.linkedfactory.core.kvin.util;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class JsonFormatWriter implements AutoCloseable {
	final JsonGenerator generator;

	URI lastItem;
	URI lastProperty;
	boolean endObject = false;

	public JsonFormatWriter(OutputStream outputStream, boolean prettyPrint) throws IOException {
		generator = JsonFormatParser.jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);
		if (prettyPrint) {
			generator.useDefaultPrettyPrinter();
		}
		generator.writeStartObject();
	}

	public JsonFormatWriter(OutputStream outputStream) throws IOException {
		this(outputStream, false);
	}

	public static String toJsonString(IExtendedIterator<KvinTuple> it) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonFormatWriter writer = new JsonFormatWriter(baos);
		try {
			while (it.hasNext()) {
				writer.writeTuple(it.next());
			}
		} finally {
			it.close();
		}
		try {
			writer.close();
		} catch (Exception e) {
			throw (e instanceof IOException ? (IOException) e : new IOException(e));
		}
		return new String(baos.toByteArray(), "UTF-8");
	}

	public void writeTuple(KvinTuple tuple) throws IOException {
		URI item = tuple.item;
		URI property = tuple.property;
		if (lastItem == null) {
			generator.writeObjectFieldStart(item.toString());
		} else if (!lastItem.equals(item)) {
			if (lastProperty != null) {
				// close last property
				generator.writeEndArray();
				// close last item
				generator.writeEndObject();
				lastProperty = null;
			}
			generator.writeObjectFieldStart(item.toString());
		}
		if (lastProperty == null) {
			generator.writeArrayFieldStart(property.toString());
		} else if (!lastProperty.equals(property)) {
			// close last property
			generator.writeEndArray();
			generator.writeArrayFieldStart(property.toString());
		}
		lastItem = item;
		lastProperty = property;

		generator.writeStartObject();
		generator.writeNumberField("time", tuple.time);
		if (tuple.seqNr > 0) {
			generator.writeNumberField("seqNr", tuple.seqNr);
		}
		generator.writeFieldName("value");
		writeValue(tuple.value);
		generator.writeEndObject();
	}

	protected void writeValue(Object value) throws IOException {
		if (value instanceof Record) {
			generator.writeStartObject();
			for (Record r : (Record) value) {
				generator.writeFieldName(r.getProperty().toString());
				writeValue(r.getValue());
			}
			generator.writeEndObject();
		} else if (value instanceof URI) {
			generator.writeStartObject();
			generator.writeStringField("@id", value.toString());
			generator.writeEndObject();
		} else {
			generator.writeObject(value);
		}
	}

	public void end() throws IOException {
		if (!endObject) {
			if (lastProperty != null) {
				generator.writeEndArray();
			}
			if (lastItem != null) {
				generator.writeEndObject();
			}
			generator.writeEndObject();
			endObject = true;
		}
	}

	@Override
	public void close() throws Exception {
		end();
		generator.close();
	}
}
