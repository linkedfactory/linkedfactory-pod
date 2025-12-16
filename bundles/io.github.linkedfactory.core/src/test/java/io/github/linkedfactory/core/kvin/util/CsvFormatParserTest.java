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

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
}
