/*
 * Copyright (c) 2024 Fraunhofer IWU.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonFormatParserTest {
	private final ObjectMapper mapper = new ObjectMapper();

	@Test
	public void shouldParseJsonResource() throws Exception {
		List<KvinTuple> tuples = parse(readResource("/JsonFormatParserTestContent.json"));

		assertEquals(11, tuples.size());
		assertTrue(tuples.get(2).value instanceof java.lang.Integer);
		assertTrue(tuples.get(3).value instanceof java.math.BigInteger);
		assertTrue(tuples.get(4).value instanceof java.lang.Double);
		assertTrue(tuples.get(5).value instanceof java.lang.Long);
		assertTrue(tuples.get(6).value instanceof java.lang.Boolean);
		assertTrue(tuples.get(7).value instanceof Record);
		assertTrue(tuples.get(10).value instanceof Record);
	}

	@Test
	public void shouldResolvePrefixesFromContext() throws Exception {
		String json = """
		{
		  "@context": {"pref": "http://test1.example/", "pref2": "http://pref2.example/"},
		  "@context": {"pref": "http://test2.example/"},
		  "pref": {
		    "pref:rest": [{"value": "val"}],
		    "pref2:rest": [{"value": "val2"}]
		  }
		}""";

		List<KvinTuple> tuples = parse(json, 1619424246100L);

		assertEquals(2, tuples.size());
		assertEquals("http://test2.example/", tuples.getFirst().item.toString());
		assertEquals("http://test2.example/rest", tuples.get(0).property.toString());
		assertEquals("val", tuples.get(0).value);
		assertEquals("http://test2.example/", tuples.get(1).item.toString());
		assertEquals("http://pref2.example/rest", tuples.get(1).property.toString());
		assertEquals("val2", tuples.get(1).value);
	}

	@Test
	public void shouldParseNestedRecords() throws Exception {
		ObjectNode root = mapper.createObjectNode();
		ObjectNode item = root.putObject("http://example.root/item");
		ArrayNode values = item.putArray("http://example.root/nested");
		ObjectNode value = values.addObject().putObject("value");
		value.put("msg", "Error 1");
		value.put("nr", 1);
		value.putObject("test_prop").put("msg", "test");
		value.putObject("id_prop").put("@id", "http://example.org/properties/test3");

		List<KvinTuple> tuples = parse(root, 1619424246100L);
		Record expectedValue = new Record(URIs.createURI("msg"), "Error 1").append(new Record(URIs.createURI("nr"), 1).append(new Record(URIs.createURI("test_prop"), new Record(URIs.createURI("msg"), "test")).append(new Record(URIs.createURI("id_prop"), URIs.createURI("http://example.org/properties/test3")))));
		List<KvinTuple> expected = List.of(new KvinTuple(URIs.createURI("http://example.root/item"), URIs.createURI("http://example.root/nested"), Kvin.DEFAULT_CONTEXT, 1619424246100L, 0, expectedValue));

		assertEquals(expected, tuples);
	}

	@Test
	public void shouldRejectMissingValueField() throws Exception {
		String json = "{ \"item1\": { \"prop1\": [ { \"seqNr\": 1 } ] } }";

		try {
			parse(json);
			fail("Expected RuntimeException due to missing 'value' field");
		} catch (RuntimeException e) {
			assertTrue(e.getCause() instanceof IOException);
		}
	}

	private List<KvinTuple> parse(JsonNode node) throws Exception {
		return parse(node, System.currentTimeMillis());
	}

	private List<KvinTuple> parse(JsonNode node, long currentTime) throws Exception {
		return parseBytes(mapper.writeValueAsBytes(node), currentTime);
	}

	private List<KvinTuple> parse(String json) throws Exception {
		return parse(json, System.currentTimeMillis());
	}

	private List<KvinTuple> parse(String json, long currentTime) throws Exception {
		return parseBytes(json.getBytes(StandardCharsets.UTF_8), currentTime);
	}

	private List<KvinTuple> parseBytes(byte[] bytes, long currentTime) throws Exception {
		JsonFormatParser parser = new JsonFormatParser(new ByteArrayInputStream(bytes));
		try (var tuples = parser.parse(currentTime)) {
			return tuples.toList();
		}
	}

	private JsonNode readResource(String path) throws Exception {
		InputStream stream = getClass().getResourceAsStream(path);
		try (stream) {
			assertNotNull(stream);
			return mapper.readTree(stream);
		}
	}
}