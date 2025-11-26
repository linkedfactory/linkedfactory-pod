package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URIs;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JsonFormatWriterTest {

	@Test
	public void shouldConvertTuplesToString() throws IOException {
		KvinTuple tuple = new KvinTuple(URIs.createURI("http://example.org/item1"), URIs.createURI("http://example.org/properties/p1"),
				null, 1619424246120L, 57.934878949512196);
		String json = JsonFormatWriter.toJsonString(WrappedIterator.create(List.of(tuple).iterator()));

		IExtendedIterator<KvinTuple> generatedTuples = new JsonFormatParser(new ByteArrayInputStream(json.getBytes())).parse();
		int count = 0;
		while (generatedTuples.hasNext()) {
			KvinTuple t = generatedTuples.next();
			assertEquals(t.item.toString(), "http://example.org/item1");
			assertEquals(t.property.toString(), "http://example.org/properties/p1");
			assertEquals(t.time, 1619424246120L);
			assertEquals(t.value.toString(), "57.934878949512196");
			count++;
		}
		assertEquals(count, 1);
	}
}
