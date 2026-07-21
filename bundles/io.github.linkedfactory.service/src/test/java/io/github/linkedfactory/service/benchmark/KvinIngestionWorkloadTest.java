package io.github.linkedfactory.service.benchmark;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.util.CsvFormatParser;
import io.github.linkedfactory.core.kvin.util.JsonFormatParser;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class KvinIngestionWorkloadTest {
	@Test
	public void suitesAreDeterministicAndSelectDisjointChannelsAndProperties() {
		List<KvinIngestionWorkload> first = KvinIngestionWorkload.variants();
		List<KvinIngestionWorkload> second = KvinIngestionWorkload.variants();

		assertEquals(KvinIngestionWorkload.VARIANT_COUNT, first.size());
		assertEquals(first.stream().map(KvinIngestionWorkload::tuples).toList(),
				second.stream().map(KvinIngestionWorkload::tuples).toList());

		Set<URI> properties = new HashSet<>();
		Set<URI> selectedChannels = new HashSet<>();
		for (int index = 0; index < first.size(); index++) {
			KvinIngestionWorkload workload = first.get(index);
			KvinIngestionWorkload copy = second.get(index);
			assertEquals(index, workload.variantIndex());
			assertEquals(workload.items(), copy.items());
			assertEquals(workload.property(), copy.property());
			assertEquals(workload.preseedTuples(), copy.preseedTuples());
			assertArrayEquals(workload.csvPayload(), copy.csvPayload());
			assertArrayEquals(workload.jsonPayload(), copy.jsonPayload());
			assertEquals(KvinIngestionWorkload.CHANNEL_COUNT, new HashSet<>(workload.items()).size());
			assertTrue("Channels overlap at variant " + index, selectedChannels.addAll(workload.items()));
			assertTrue("Property repeated at variant " + index, properties.add(workload.property()));
		}
		assertEquals(KvinIngestionWorkload.VARIANT_COUNT, properties.size());
		assertEquals(KvinIngestionWorkload.VARIANT_COUNT * KvinIngestionWorkload.CHANNEL_COUNT,
				selectedChannels.size());
		assertEquals(first.get(0).tuples(), new KvinIngestionWorkload().tuples());
	}

	@Test
	public void everyVariantHasTheExpectedShapeAndDisjointKeysAndWindows() {
		Set<String> allKeys = new HashSet<>();
		Set<Long> allTimestamps = new HashSet<>();

		for (KvinIngestionWorkload workload : KvinIngestionWorkload.variants()) {
			long expectedStartTime = KvinIngestionWorkload.BASE_START_TIME
					+ workload.variantIndex() * KvinIngestionWorkload.TIMESTAMP_WINDOW_SIZE;
			assertEquals(KvinIngestionWorkload.TUPLE_COUNT, workload.tuples().size());
			assertEquals(KvinIngestionWorkload.CHANNEL_COUNT, workload.preseedTuples().size());

			Map<Long, Set<Integer>> sequencesByTimestamp = new HashMap<>();
			Set<Long> variantTimestamps = new HashSet<>();
			for (KvinTuple tuple : workload.tuples()) {
				assertEquals(workload.property(), tuple.property);
				assertEquals(KvinIngestionWorkload.CONTEXT, tuple.context);
				assertTrue(tuple.seqNr >= 1 && tuple.seqNr <= KvinIngestionWorkload.SEQUENCES_PER_TIMESTAMP);
				assertTrue("Duplicate tuple key", allKeys.add(key(tuple)));
				variantTimestamps.add(tuple.time);
				sequencesByTimestamp.computeIfAbsent(tuple.time, ignored -> new HashSet<>()).add(tuple.seqNr);
			}

			assertEquals(KvinIngestionWorkload.TIMESTAMP_COUNT, variantTimestamps.size());
			assertTrue("Timestamp windows overlap", allTimestamps.stream().noneMatch(variantTimestamps::contains));
			allTimestamps.addAll(variantTimestamps);
			assertEquals(KvinIngestionWorkload.TIMESTAMP_COUNT, sequencesByTimestamp.size());
			assertTrue(sequencesByTimestamp.values().stream()
					.allMatch(sequences -> sequences.size() == KvinIngestionWorkload.SEQUENCES_PER_TIMESTAMP));
			assertEquals(expectedStartTime, variantTimestamps.stream().mapToLong(Long::longValue).min().orElseThrow());
			assertEquals(expectedStartTime + (KvinIngestionWorkload.TIMESTAMP_COUNT - 1)
					* KvinIngestionWorkload.TIMESTAMP_STEP,
					variantTimestamps.stream().mapToLong(Long::longValue).max().orElseThrow());
		}
	}

	@Test
	public void allPayloadFormsNormalizeToEachVariantsCanonicalTupleSet() throws Exception {
		for (KvinIngestionWorkload workload : KvinIngestionWorkload.variants()) {
			Set<KvinTuple> expected = new HashSet<>(workload.tuples());

			List<KvinTuple> csv = parseCsv(workload.csvPayload());
			assertEquals("CSV tuple order for variant " + workload.variantIndex(), workload.tuples(), csv);
			assertEquals(expected, new HashSet<>(csv));

			List<KvinTuple> json = parseJson(workload.jsonPayload());
			assertEquals(KvinIngestionWorkload.TUPLE_COUNT, json.size());
			assertEquals(expected, new HashSet<>(json));
			assertJsonOrder(json);

			List<KvinTuple> partitionedCsv = new ArrayList<>();
			List<byte[]> payloads = workload.csvPayloads(10);
			assertEquals(10, payloads.size());
			for (byte[] payload : payloads) {
				partitionedCsv.addAll(parseCsv(payload));
			}
			assertEquals(KvinIngestionWorkload.TUPLE_COUNT, partitionedCsv.size());
			assertEquals(workload.tuples(), partitionedCsv);
			assertEquals(expected, new HashSet<>(partitionedCsv));

			assertEquals(KvinIngestionWorkload.ROW_COUNT + 1,
					new String(workload.csvPayload(), StandardCharsets.UTF_8).split("\\n").length);
			assertNotEquals(0, workload.jsonPayload().length);
		}
	}

	private static List<KvinTuple> parseCsv(byte[] payload) throws Exception {
		List<KvinTuple> tuples = new ArrayList<>();
		CsvFormatParser parser = new CsvFormatParser(URIs.createURI("http://foo.com/linkedfactory/"), ',',
				new ByteArrayInputStream(payload));
		parser.setContext(KvinIngestionWorkload.CONTEXT);
		try (IExtendedIterator<KvinTuple> iterator = parser.parse()) {
			while (iterator.hasNext()) {
				tuples.add(iterator.next());
			}
		}
		return tuples;
	}

	private static List<KvinTuple> parseJson(byte[] payload) throws Exception {
		return new JsonFormatParser(new ByteArrayInputStream(payload))
				.setContext(KvinIngestionWorkload.CONTEXT)
				.parse().toList();
	}

	private static void assertJsonOrder(List<KvinTuple> tuples) {
		Comparator<KvinTuple> order = Comparator.comparing((KvinTuple tuple) -> tuple.item.toString())
				.thenComparing(tuple -> tuple.property.toString())
				.thenComparingLong(tuple -> tuple.time)
				.thenComparingInt(tuple -> tuple.seqNr);
		for (int index = 1; index < tuples.size(); index++) {
			assertFalse("JSON is out of order at tuple " + index,
					order.compare(tuples.get(index - 1), tuples.get(index)) > 0);
		}
	}

	private static String key(KvinTuple tuple) {
		return tuple.context + "|" + tuple.item + "|" + tuple.property + "|" + tuple.time + "|" + tuple.seqNr;
	}
}
