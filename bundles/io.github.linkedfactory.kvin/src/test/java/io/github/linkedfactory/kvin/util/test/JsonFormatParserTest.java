package io.github.linkedfactory.kvin.util.test;

import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.kvin.util.JsonFormatParser;
import net.enilink.commons.iterator.IExtendedIterator;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class JsonFormatParserTest {
    @Test
    public void shouldParseJson() {
        try {

            JsonFormatParser jsonParser = new JsonFormatParser(getClass().getClassLoader().getResourceAsStream("JsonFormatParserTestContent.json"));
            IExtendedIterator<KvinTuple> tuples = jsonParser.parse();
            assertNotNull(tuples);
            int index = 0;
            while (tuples.hasNext()) {
                KvinTuple t = tuples.next();
                if (index == 2) {
                    assertTrue(t.value instanceof Integer);
                } else if (index == 3) {
                    assertTrue(t.value instanceof BigInteger);
                } else if (index == 4) {
                    assertTrue(t.value instanceof BigDecimal);
                } else if (index == 5) {
                    assertTrue(t.value instanceof Long);
                } else if (index == 6) {
                    assertTrue(t.value instanceof Boolean);
                } else if (index == 7 || index == 10) {
                    assertTrue(t.value instanceof Record);
                }
                index++;
            }
            assertEquals(index, 11);


        } catch (Exception e) {
            fail("Something went wrong while testing JsonFormatParser parse() method");
        }
    }
}
