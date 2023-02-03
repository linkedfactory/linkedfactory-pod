package io.github.linkedfactory.kvin.util;

import io.github.linkedfactory.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JsonFormatWriterTest {
    @Test
    public void shouldConvertTuplesToString() {
        try {
            KvinTuple tuple = new KvinTuple(URIs.createURI("http://example.org/item1"), URIs.createURI("http://example.org/properties/p1"), null, 1619424246120l, 57.934878949512196);
            String json = new JsonFormatWriter().toJsonString(WrappedIterator.create(new ArrayList<>(Arrays.asList(tuple)).iterator()));

            IExtendedIterator<KvinTuple> generatedTuples = new JsonFormatParser(new ByteArrayInputStream(json.getBytes())).parse();
            int count = 0;
            while (generatedTuples.hasNext()) {
                KvinTuple t = generatedTuples.next();
                assertEquals(t.item.toString(), "http://example.org/item1");
                assertEquals(t.property.toString(), "http://example.org/properties/p1");
                assertEquals(t.time, 1619424246120l);
                assertEquals(t.value.toString(), "57.934878949512196");
                count++;
            }
            assertEquals(count, 1);

        } catch (Exception e) {
            fail("Something went wrong while testing JsonFormatParser toJsonString() method");
        }
    }
}
