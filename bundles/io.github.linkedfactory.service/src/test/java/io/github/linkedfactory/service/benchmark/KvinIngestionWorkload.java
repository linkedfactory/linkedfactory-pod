package io.github.linkedfactory.service.benchmark;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.util.JsonFormatWriter;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public final class KvinIngestionWorkload {
	public static final int ROW_COUNT = 5_000;
	public static final int CHANNEL_COUNT = 6;
	public static final int TUPLE_COUNT = ROW_COUNT * CHANNEL_COUNT;
	public static final int TIMESTAMP_COUNT = 1_000;
	public static final int SEQUENCES_PER_TIMESTAMP = 5;
	public static final int VARIANT_COUNT = 10;
	public static final int CHANNEL_POOL_SIZE = 100;
	public static final int PROPERTY_POOL_SIZE = 10;
	public static final long BASE_START_TIME = 1_710_000_000_000L;
	public static final long TIMESTAMP_STEP = 1_000L;
	public static final long TIMESTAMP_WINDOW_SIZE = TIMESTAMP_COUNT * TIMESTAMP_STEP;
	public static final long SHUFFLE_SEED = 0x4B56494E_20260721L;

	public static final URI CONTEXT = URIs.createURI("http://iwu.lf.de/ecc4p/models/emag");

	private static final List<URI> CHANNEL_POOL = createUriPool("http://iwu.lf.de/ecc4p/emag/channel-", CHANNEL_POOL_SIZE, 3);
	private static final List<URI> PROPERTY_POOL = createUriPool("http://iwu.lf.de/ecc4p/property-", PROPERTY_POOL_SIZE, 2);
	private static final List<URI> SHUFFLED_CHANNELS = shuffledChannels();

	private final int variantIndex;
	private final List<URI> items;
	private final URI property;
	private final long startTime;
	private final List<KvinTuple> tuples;
	private final List<KvinTuple> preseedTuples;
	private final byte[] jsonPayload;
	private final byte[] csvPayload;

	public KvinIngestionWorkload() {
		this(0);
	}

	KvinIngestionWorkload(int variantIndex) {
		if (variantIndex < 0 || variantIndex >= VARIANT_COUNT) {
			throw new IllegalArgumentException("variantIndex must be in [0, " + VARIANT_COUNT + "): " + variantIndex);
		}
		this.variantIndex = variantIndex;
		int firstChannel = variantIndex * CHANNEL_COUNT;
		this.items = List.copyOf(SHUFFLED_CHANNELS.subList(firstChannel, firstChannel + CHANNEL_COUNT));
		this.property = PROPERTY_POOL.get(variantIndex);
		this.startTime = BASE_START_TIME + variantIndex * TIMESTAMP_WINDOW_SIZE;
		this.tuples = createTuples();
		this.preseedTuples = createPreseedTuples();
		this.jsonPayload = createJsonPayload(tuples);
		this.csvPayload = createCsvPayload(0, ROW_COUNT);
	}

	static List<KvinIngestionWorkload> variants() {
		List<KvinIngestionWorkload> variants = new ArrayList<>(VARIANT_COUNT);
		for (int variant = 0; variant < VARIANT_COUNT; variant++) {
			variants.add(new KvinIngestionWorkload(variant));
		}
		return List.copyOf(variants);
	}

	int variantIndex() {
		return variantIndex;
	}

	List<URI> items() {
		return items;
	}

	URI property() {
		return property;
	}

	public List<KvinTuple> tuples() {
		return tuples;
	}

	public List<KvinTuple> preseedTuples() {
		return preseedTuples;
	}

	public byte[] jsonPayload() {
		return jsonPayload.clone();
	}

	public byte[] csvPayload() {
		return csvPayload.clone();
	}

	public List<byte[]> csvPayloads(int fileCount) {
		if (fileCount <= 0 || ROW_COUNT % fileCount != 0) {
			throw new IllegalArgumentException("fileCount must divide " + ROW_COUNT + ": " + fileCount);
		}
		int rowsPerFile = ROW_COUNT / fileCount;
		List<byte[]> payloads = new ArrayList<>(fileCount);
		for (int file = 0; file < fileCount; file++) {
			int startRow = file * rowsPerFile;
			payloads.add(createCsvPayload(startRow, startRow + rowsPerFile));
		}
		return List.copyOf(payloads);
	}

	private List<KvinTuple> createTuples() {
		List<KvinTuple> result = new ArrayList<>(TUPLE_COUNT);
		for (int row = 0; row < ROW_COUNT; row++) {
			for (int channel = 0; channel < CHANNEL_COUNT; channel++) {
				long time = startTime + (row / SEQUENCES_PER_TIMESTAMP) * TIMESTAMP_STEP;
				int seqNr = row % SEQUENCES_PER_TIMESTAMP + 1;
				result.add(new KvinTuple(items.get(channel), property, CONTEXT, time, seqNr, value(channel, row)));
			}
		}
		return List.copyOf(result);
	}

	private List<KvinTuple> createPreseedTuples() {
		List<KvinTuple> result = new ArrayList<>(CHANNEL_COUNT);
		for (int channel = 0; channel < CHANNEL_COUNT; channel++) {
			result.add(new KvinTuple(items.get(channel), property, CONTEXT, startTime - TIMESTAMP_STEP, 0,
					value(channel, -1)));
		}
		return List.copyOf(result);
	}

	private static byte[] createJsonPayload(List<KvinTuple> tuples) {
		try {
			String json = JsonFormatWriter.toJsonString(WrappedIterator.create(tuples.stream()
					.sorted(Comparator.comparing((KvinTuple t) -> t.item.toString())
							.thenComparing(t -> t.property.toString())
							.thenComparingLong(t -> t.time)
							.thenComparingInt(t -> t.seqNr))
					.toList().iterator()));
			return json.getBytes(StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private byte[] createCsvPayload(int startRow, int endRow) {
		StringBuilder csv = new StringBuilder((endRow - startRow) * CHANNEL_COUNT * 12);
		csv.append("time,seqNr");
		for (URI item : items) {
			csv.append(',').append(item).append('@').append(property);
		}
		csv.append('\n');

		for (int row = startRow; row < endRow; row++) {
			csv.append(startTime + (row / SEQUENCES_PER_TIMESTAMP) * TIMESTAMP_STEP)
					.append(',').append(row % SEQUENCES_PER_TIMESTAMP + 1);
			for (int channel = 0; channel < CHANNEL_COUNT; channel++) {
				csv.append(',').append(value(channel, row));
			}
			csv.append('\n');
		}
		return csv.toString().getBytes(StandardCharsets.UTF_8);
	}

	private static List<URI> createUriPool(String prefix, int size, int digits) {
		List<URI> uris = new ArrayList<>(size);
		for (int index = 0; index < size; index++) {
			uris.add(URIs.createURI(prefix + String.format(Locale.ROOT, "%0" + digits + "d", index)));
		}
		return List.copyOf(uris);
	}

	private static List<URI> shuffledChannels() {
		List<URI> channels = new ArrayList<>(CHANNEL_POOL);
		Collections.shuffle(channels, new Random(SHUFFLE_SEED));
		return List.copyOf(channels);
	}

	private static double value(int channel, int row) {
		return channel * 100_000.0 + row + 0.25;
	}
}
