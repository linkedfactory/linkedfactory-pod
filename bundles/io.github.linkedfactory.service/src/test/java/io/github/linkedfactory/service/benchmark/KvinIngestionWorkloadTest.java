package io.github.linkedfactory.service.benchmark;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.util.CsvFormatParser;
import io.github.linkedfactory.core.kvin.util.JsonFormatParser;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

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
		try (IExtendedIterator<KvinTuple> csvIterator = csvParser.parse()) {
			while (csvIterator.hasNext()) {
				KvinTuple tuple = csvIterator.next();
				assertEquals("CSV tuple order", workload.tuples().get(csvCount), tuple);
				csvTuples.add(tuple);
				csvCount++;
			}
		}
		assertEquals("CSV tuple count", KvinIngestionWorkload.TUPLE_COUNT, csvCount);
		assertEquals(expected, csvTuples);

		java.util.List<KvinTuple> json = parseJson(new String(workload.jsonPayload(), StandardCharsets.UTF_8));
		assertEquals("JSON tuple count", KvinIngestionWorkload.TUPLE_COUNT, json.size());
		Set<KvinTuple> jsonTuples = new HashSet<>(json);
		Set<KvinTuple> missing = new HashSet<>(expected);
		missing.removeAll(jsonTuples);
		Set<KvinTuple> extra = new HashSet<>(jsonTuples);
		extra.removeAll(expected);
		assertEquals(expected.size(), jsonTuples.size());
		assertTrue("JSON missing=" + sample(missing) + ", extra=" + sample(extra), missing.isEmpty() && extra.isEmpty());
		assertEquals(KvinIngestionWorkload.ROW_COUNT + 1,
				new String(workload.csvPayload(), StandardCharsets.UTF_8).split("\\n").length);
		assertNotEquals(0, workload.jsonPayload().length);
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
				try (IExtendedIterator<KvinTuple> tuples = parser.parse()) {
					while (tuples.hasNext()) {
						actual.add(tuples.next());
						tupleCount++;
					}
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

	private static java.util.List<KvinTuple> parseJson(String json) throws Exception {
		return new JsonFormatParser(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)))
				.setContext(KvinIngestionWorkload.CONTEXT)
				.parse().toList();
	}
}