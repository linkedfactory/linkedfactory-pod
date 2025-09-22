/*
 * Copyright (c) 2023 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import net.enilink.commons.iterator.IExtendedIterator;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class JsonFormatParserTest {

    @Test
    public void shouldParseJson() throws IOException {
        JsonFormatParser jsonParser = new JsonFormatParser(
            getClass().getClassLoader().getResourceAsStream("JsonFormatParserTestContent.json"));
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
                assertTrue(t.value instanceof Double);
            } else if (index == 5) {
                assertTrue(t.value instanceof Long);
            } else if (index == 6) {
                assertTrue(t.value instanceof Boolean);
            } else if (index == 7 || index == 10) {
                assertTrue(t.value instanceof Record);
            }
            index++;
        }
        assertEquals(11, index);
    }

    @Test
    public void shouldThrowOnMalformedJson() {
        String malformedJson = "{ \"item1\": { \"prop1\": [ { \"value\": 123 } ] "; // missing closing braces
        try {
            JsonFormatParser parser = new JsonFormatParser(
                new ByteArrayInputStream(malformedJson.getBytes(StandardCharsets.UTF_8)));
            var it = parser.parse();
			while (it.hasNext()) {
				it.next();
			}
            fail("Expected RuntimeException due to malformed JSON");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void shouldThrowOnMissingValueField() {
        String missingValueJson = "{ \"item1\": { \"prop1\": [ { \"seqNr\": 1 } ] } }";
        try {
            JsonFormatParser parser = new JsonFormatParser(
                new ByteArrayInputStream(missingValueJson.getBytes(StandardCharsets.UTF_8)));
            parser.parse().hasNext();
            fail("Expected RuntimeException due to missing 'value' field");
        } catch (Exception e) {
	        assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void shouldThrowOnEmptyItemName() {
        String emptyItemNameJson = "{ \"\": { \"prop1\": [ { \"value\": 1 } ] } }";
        try {
            JsonFormatParser parser = new JsonFormatParser(
                new ByteArrayInputStream(emptyItemNameJson.getBytes(StandardCharsets.UTF_8)));
            parser.parse().hasNext();
            fail("Expected RuntimeException due to empty item name");
        } catch (Exception e) {
	        assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void shouldThrowOnEmptyPropertyName() {
        String emptyPropertyNameJson = "{ \"item1\": { \"\": [ { \"value\": 1 } ] } }";
        try {
            JsonFormatParser parser = new JsonFormatParser(
                new ByteArrayInputStream(emptyPropertyNameJson.getBytes(StandardCharsets.UTF_8)));
            parser.parse().hasNext();
            fail("Expected RuntimeException due to empty property name");
        } catch (Exception e) {
	        assertTrue(e.getCause() instanceof IOException);
        }
    }

	@Test
	public void testUriWithSpaces() {
		String jsonWithSpacesInUri = "{ \"http://example.com/item 1\": { \"http://example.com/prop 1\": [ { \"value\": 1 } ] } }";
		try {
			JsonFormatParser parser = new JsonFormatParser(
				new ByteArrayInputStream(jsonWithSpacesInUri.getBytes(StandardCharsets.UTF_8)));
			parser.parse().hasNext();
			fail("Expected RuntimeException due to spaces in URI");
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof IOException);
		}
	}
}
