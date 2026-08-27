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

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

public class CsvFormatParserTest {

	@Test
	public void shouldParseCsv1() throws IOException {
		CsvFormatParser csvParser = new CsvFormatParser(URIs.createURI("urn:base:"), ';',
				getClass().getResourceAsStream("/CsvFormatParserTestContent-1.csv"));
		IExtendedIterator<KvinTuple> tuples = csvParser.parse();
		assertNotNull(tuples);
		int count = 0;
		while (tuples.hasNext()) {
			KvinTuple t = tuples.next();
			count++;
		}
		assertEquals(176, count);
	}

	@Test
	public void shouldParseCsv2() throws IOException {
		for (String[] sepAndFile : List.of(
				new String[]{",", "/CsvFormatParserTestContent-2a.csv"},
				new String[]{";", "/CsvFormatParserTestContent-2b.csv"}
		)) {
			CsvFormatParser csvParser = new CsvFormatParser(URIs.createURI("urn:base:"), sepAndFile[0].charAt(0),
					getClass().getResourceAsStream(sepAndFile[1]));
			IExtendedIterator<KvinTuple> tuples = csvParser.parse();
			assertNotNull(tuples);
			int count = 0;
			while (tuples.hasNext()) {
				KvinTuple t = tuples.next();
				count++;
			}
			assertEquals(36, count);
		}
	}

	@Test
	public void shouldParseCsvWithSeqNr() throws IOException {
		for (String[] sepAndFile : List.of(
				new String[]{",", "/CsvFormatParserTestContent-2a-seqNr-1.csv"},
				new String[]{";", "/CsvFormatParserTestContent-2b-seqNr-5.csv"}
		)) {
			CsvFormatParser csvParser = new CsvFormatParser(URIs.createURI("urn:base:"), sepAndFile[0].charAt(0),
					getClass().getResourceAsStream(sepAndFile[1]));
			IExtendedIterator<KvinTuple> tuples = csvParser.parse();
			assertNotNull(tuples);
			int count = 0;
			while (tuples.hasNext()) {
				KvinTuple t = tuples.next();
				count++;
			}
			assertEquals(36, count);
		}
	}

	@Test
	public void shouldGenerateCorrectItemAndPropertyNames() throws IOException {
		String baseStr = "urn:base:";
		// header: time, full-item@full-prop, full-item2@full-prop2, propWithoutItem
		String csv = String.join("\n",
				"time,<http://ex/item1>@<http://ex/prop1>,<http://ex/item2>@<http://ex/prop2>,prop3",
				"123456,1,2,3"
		);
		ByteArrayInputStream in = new ByteArrayInputStream(csv.getBytes());
		CsvFormatParser csvParser = new CsvFormatParser(URIs.createURI(baseStr), ',', in);
		IExtendedIterator<KvinTuple> tuples = csvParser.parse();
		assertNotNull(tuples);

		// first tuple -> item http://ex/item1 property http://ex/prop1
		assertTrue(tuples.hasNext());
		KvinTuple t1 = tuples.next();
		assertEquals(URIs.createURI("http://ex/item1"), t1.item);
		assertEquals(URIs.createURI("http://ex/prop1"), t1.property);

		// second tuple -> item http://ex/item2 property http://ex/prop2
		assertTrue(tuples.hasNext());
		KvinTuple t2 = tuples.next();
		assertEquals(URIs.createURI("http://ex/item2"), t2.item);
		assertEquals(URIs.createURI("http://ex/prop2"), t2.property);

		// third tuple -> item is base (prop without item) property prop3 (relative -> appended to base)
		assertTrue(tuples.hasNext());
		KvinTuple t3 = tuples.next();
		assertEquals(URIs.createURI(baseStr), t3.item);
		// header "prop3" is relative and should be appended to base as local part
		assertEquals(URIs.createURI(baseStr).appendLocalPart("prop3"), t3.property);

		// no more tuples
		assertFalse(tuples.hasNext());
	}

	@Test
	public void shouldParseCsvDoubleValues() throws IOException {
		String baseStr = "urn:base:";
		// header: time,prop -> values 0.0 and 3.0 should be parsed as Double
		String csv = String.join("\n",
				"time,prop",
				"123,0.0",
				"124,3.0"
		);
		ByteArrayInputStream in = new ByteArrayInputStream(csv.getBytes());
		CsvFormatParser csvParser = new CsvFormatParser(URIs.createURI(baseStr), ',', in);
		IExtendedIterator<KvinTuple> tuples = csvParser.parse();
		assertNotNull(tuples);

		// first tuple -> value 0.0 as Double
		assertTrue(tuples.hasNext());
		KvinTuple t1 = tuples.next();
		assertNotNull(t1);
		assertTrue(t1.value instanceof Double);
		assertEquals(0.0, ((Double) t1.value), 0.0);

		// second tuple -> value 3.0 as Double
		assertTrue(tuples.hasNext());
		KvinTuple t2 = tuples.next();
		assertNotNull(t2);
		assertTrue(t2.value instanceof Double);
		assertEquals(3.0, ((Double) t2.value), 0.0);

		// no more tuples
		assertFalse(tuples.hasNext());
	}

	@Test
	public void shouldPreserveSparseRowsAndTupleFields() throws IOException {
		String csv = String.join("\n",
				"time,seqNr,<urn:item:a>@<urn:property>,<urn:item:b>@<urn:property>",
				"100,1,10",
				"101,2,,20");
		CsvFormatParser parser = new CsvFormatParser(URIs.createURI("urn:base:"), ',',
				new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));
		List<KvinTuple> tuples = parser.parse().toList();

		assertEquals(3, tuples.size());
		assertTuple(tuples.get(0), "urn:item:a", 100, 1, 10L);
		assertTuple(tuples.get(1), "urn:item:a", 101, 2, "");
		assertTuple(tuples.get(2), "urn:item:b", 101, 2, 20L);
	}

	@Test
	public void shouldRejectMalformedTimeAndSequenceNumberAndCloseInput() throws IOException {
		assertMalformedCsv("time,value\ninvalid,1", "Invalid time format");
		assertMalformedCsv("time,seqNr,value\n100,invalid,1", "Invalid seqNr format");
	}

	@Test
	public void shouldCloseInputAtEndOfFile() throws IOException {
		TrackingInputStream input = new TrackingInputStream("time,value\n100,1");
		CsvFormatParser parser = new CsvFormatParser(URIs.createURI("urn:base:"), ',', input);
		assertEquals(1, parser.parse().toList().size());
		assertTrue(input.closed);
	}

	private static void assertMalformedCsv(String csv, String expectedMessage) throws IOException {
		TrackingInputStream input = new TrackingInputStream(csv);
		CsvFormatParser parser = new CsvFormatParser(URIs.createURI("urn:base:"), ',', input);
		RuntimeException error = assertThrows(RuntimeException.class, () -> parser.parse().hasNext());
		assertTrue(error.getCause().getMessage().contains(expectedMessage));
		assertTrue(input.closed);
	}

	private static void assertTuple(KvinTuple tuple, String item, long time, int seqNr, Object value) {
		assertEquals(URIs.createURI(item), tuple.item);
		assertEquals(URIs.createURI("urn:property"), tuple.property);
		assertEquals(time, tuple.time);
		assertEquals(seqNr, tuple.seqNr);
		assertEquals(value, tuple.value);
	}

	private static final class TrackingInputStream extends ByteArrayInputStream {
		private boolean closed;

		private TrackingInputStream(String content) {
			super(content.getBytes(StandardCharsets.UTF_8));
		}

		@Override
		public void close() throws IOException {
			closed = true;
			super.close();
		}
	}

}
