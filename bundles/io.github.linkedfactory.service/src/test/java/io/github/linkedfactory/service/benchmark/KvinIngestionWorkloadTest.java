package io.github.linkedfactory.service.benchmark;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.util.CsvFormatParser;
import io.github.linkedfactory.service.util.JsonFormatParser;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URIs;
import net.liftweb.common.Box;
import org.json4s.AsJsonInput;
import org.json4s.JValue;
import org.junit.Test;
import scala.collection.immutable.List;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KvinIngestionWorkloadTest {
	@Test
	public void hasTheExpectedDeterministicShape() {
		KvinIngestionWorkload workload = new KvinIngestionWorkload();
		assertEquals(KvinIngestionWorkload.TUPLE_COUNT, workload.tuples().size());
		assertEquals(KvinIngestionWorkload.CHANNEL_COUNT, new HashSet<>(workload.tuples().stream()
				.map(tuple -> tuple.item).toList()).size());
		assertEquals(KvinIngestionWorkload.TUPLE_COUNT + KvinIngestionWorkload.CHANNEL_COUNT,
				workload.tuples().size() + workload.preseedTuples().size());

		Map<Long, Set<Integer>> sequencesByTimestamp = new HashMap<>();
		Set<String> keys = new HashSet<>();
		for (KvinTuple tuple : workload.tuples()) {
			assertEquals(KvinIngestionWorkload.PROPERTY, tuple.property);
			assertEquals(KvinIngestionWorkload.CONTEXT, tuple.context);
			assertTrue(tuple.seqNr >= 1 && tuple.seqNr <= KvinIngestionWorkload.SEQUENCES_PER_TIMESTAMP);
			assertTrue(keys.add(key(tuple)));
			sequencesByTimestamp.computeIfAbsent(tuple.time, ignored -> new HashSet<>()).add(tuple.seqNr);
		}
		assertEquals(KvinIngestionWorkload.TIMESTAMP_COUNT, sequencesByTimestamp.size());
		assertTrue(sequencesByTimestamp.values().stream()
				.allMatch(sequences -> sequences.size() == KvinIngestionWorkload.SEQUENCES_PER_TIMESTAMP));
		assertEquals(workload.tuples(), new KvinIngestionWorkload().tuples());
	}

	@Test
	public void payloadsNormalizeToTheCanonicalTupleSet() throws Exception {
		KvinIngestionWorkload workload = new KvinIngestionWorkload();
		Set<KvinTuple> expected = new HashSet<>(workload.tuples());

		Set<KvinTuple> csvTuples = new HashSet<>();
		int csvCount = 0;
		CsvFormatParser csvParser = new CsvFormatParser(URIs.createURI("http://foo.com/linkedfactory/"), ',',
				new ByteArrayInputStream(workload.csvPayload()));
		csvParser.setContext(KvinIngestionWorkload.CONTEXT);
		IExtendedIterator<KvinTuple> csvIterator = csvParser.parse();
		try {
			while (csvIterator.hasNext()) {
				KvinTuple tuple = csvIterator.next();
				assertEquals("CSV tuple order", workload.tuples().get(csvCount), tuple);
				csvTuples.add(tuple);
				csvCount++;
			}
		} finally {
			csvIterator.close();
		}
		assertEquals("CSV tuple count", KvinIngestionWorkload.TUPLE_COUNT, csvCount);
		assertEquals(expected, csvTuples);

		JValue json = parseJson(new String(workload.jsonPayload(), StandardCharsets.UTF_8));
		Box<List<KvinTuple>> parsed = parseProductionJson(json);
		assertTrue(parsed.isDefined());
		Set<KvinTuple> jsonTuples = new HashSet<>();
		@SuppressWarnings("unchecked")
		List<KvinTuple> parsedTuples = (List<KvinTuple>) parsed.openOr(null);
		assertEquals("JSON tuple count", KvinIngestionWorkload.TUPLE_COUNT, parsedTuples.size());
		scala.collection.Iterator<KvinTuple> jsonIterator = parsedTuples.iterator();
		while (jsonIterator.hasNext()) {
			jsonTuples.add(jsonIterator.next());
		}
		Set<KvinTuple> missing = new HashSet<>(expected);
		missing.removeAll(jsonTuples);
		Set<KvinTuple> extra = new HashSet<>(jsonTuples);
		extra.removeAll(expected);
		assertEquals(expected.size(), jsonTuples.size());
		assertTrue("JSON missing=" + sample(missing) + ", extra=" + sample(extra), missing.isEmpty() && extra.isEmpty());
		assertEquals(KvinIngestionWorkload.ROW_COUNT + 1,
				new String(workload.csvPayload(), StandardCharsets.UTF_8).split("\\n").length);
		assertFalse(workload.jsonPayload().length == 0);
	}

	@Test
	public void csvFilePartitionsNormalizeToTheCanonicalTupleSet() throws Exception {
		KvinIngestionWorkload workload = new KvinIngestionWorkload();
		Set<KvinTuple> expected = new HashSet<>(workload.tuples());

		for (int fileCount : java.util.List.of(1, 2, 5, 10)) {
			Set<KvinTuple> actual = new HashSet<>();
			int tupleCount = 0;
			java.util.List<byte[]> payloads = workload.csvPayloads(fileCount);
			for (byte[] payload : payloads) {
				CsvFormatParser parser = new CsvFormatParser(URIs.createURI("http://foo.com/linkedfactory/"), ',',
						new ByteArrayInputStream(payload));
				parser.setContext(KvinIngestionWorkload.CONTEXT);
				IExtendedIterator<KvinTuple> tuples = parser.parse();
				try {
					while (tuples.hasNext()) {
						actual.add(tuples.next());
						tupleCount++;
					}
				} finally {
					tuples.close();
				}
			}

			assertEquals(fileCount, payloads.size());
			assertEquals(KvinIngestionWorkload.TUPLE_COUNT, tupleCount);
			assertEquals(expected, actual);
		}
	}

	private static String key(KvinTuple tuple) {
		return tuple.context + "|" + tuple.item + "|" + tuple.property + "|" + tuple.time + "|" + tuple.seqNr;
	}

	private static String sample(Set<KvinTuple> tuples) {
		return tuples.stream().limit(3).toList().toString();
	}

	private static JValue parseJson(String json) throws Exception {
		Class<?> parserClass = Class.forName("org.json4s.native.JsonParser$");
		Object parser = parserClass.getField("MODULE$").get(null);
		Method parse = parserClass.getMethod("parse", java.io.Reader.class, boolean.class, boolean.class,
				boolean.class);
		return (JValue) parse.invoke(parser, new StringReader(json), true, false, true);
	}

	@SuppressWarnings("unchecked")
	private static Box<List<KvinTuple>> parseProductionJson(JValue json) throws Exception {
		Class<?> parserClass = Class.forName("io.github.linkedfactory.service.util.JsonFormatParser$");
		Object parser = parserClass.getField("MODULE$").get(null);
		Method parseItem = parserClass.getMethod("parseItem", net.enilink.komma.core.URI.class,
				net.enilink.komma.core.URI.class, JValue.class, long.class);
		return (Box<List<KvinTuple>>) parseItem.invoke(parser,
				URIs.createURI("http://foo.com/linkedfactory/"), KvinIngestionWorkload.CONTEXT,
				json, KvinIngestionWorkload.START_TIME);
	}
}