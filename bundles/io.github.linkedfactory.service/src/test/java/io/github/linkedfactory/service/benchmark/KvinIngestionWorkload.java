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
import java.util.List;

public final class KvinIngestionWorkload {
	public static final int ROW_COUNT = 5_000;
	public static final int CHANNEL_COUNT = 6;
	public static final int TUPLE_COUNT = ROW_COUNT * CHANNEL_COUNT;
	public static final int TIMESTAMP_COUNT = 1_000;
	public static final int SEQUENCES_PER_TIMESTAMP = 5;
	public static final long START_TIME = 1_710_000_000_000L;

	public static final URI PROPERTY = URIs.createURI("http://iwu.lf.de/ecc4p/values");
	public static final URI CONTEXT = URIs.createURI("http://iwu.lf.de/ecc4p/models/emag");
	public static final List<URI> ITEMS = List.of(
			URIs.createURI("http://iwu.lf.de/ecc4p/emag/channel-1"),
			URIs.createURI("http://iwu.lf.de/ecc4p/emag/channel-2"),
			URIs.createURI("http://iwu.lf.de/ecc4p/emag/channel-3"),
			URIs.createURI("http://iwu.lf.de/ecc4p/emag/channel-4"),
			URIs.createURI("http://iwu.lf.de/ecc4p/emag/channel-5"),
			URIs.createURI("http://iwu.lf.de/ecc4p/emag/channel-6"));

	private final List<KvinTuple> tuples;
	private final List<KvinTuple> preseedTuples;
	private final byte[] jsonPayload;
	private final byte[] csvPayload;

	public KvinIngestionWorkload() {
		this.tuples = createTuples();
		this.preseedTuples = createPreseedTuples();
		this.jsonPayload = createJsonPayload(tuples);
		this.csvPayload = createCsvPayload();
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

	private static List<KvinTuple> createTuples() {
		List<KvinTuple> tuples = new ArrayList<>(TUPLE_COUNT);
		for (int row = 0; row < ROW_COUNT; row++) {
			for (int channel = 0; channel < CHANNEL_COUNT; channel++) {
				long time = START_TIME + row / SEQUENCES_PER_TIMESTAMP;
				int seqNr = row % SEQUENCES_PER_TIMESTAMP + 1;
				double value = value(channel, row);
				tuples.add(new KvinTuple(ITEMS.get(channel), PROPERTY, CONTEXT, time, seqNr, value));
			}
		}
		return List.copyOf(tuples);
	}

	private static List<KvinTuple> createPreseedTuples() {
		List<KvinTuple> tuples = new ArrayList<>(CHANNEL_COUNT);
		for (int channel = 0; channel < CHANNEL_COUNT; channel++) {
			tuples.add(new KvinTuple(ITEMS.get(channel), PROPERTY, CONTEXT, START_TIME - 1, 0,
					value(channel, -1)));
		}
		return List.copyOf(tuples);
	}

	private static byte[] createJsonPayload(List<KvinTuple> tuples) {
		try {
			String json = JsonFormatWriter.toJsonString(WrappedIterator.create(tuples.iterator()));
			return json.getBytes(StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static byte[] createCsvPayload() {
		return createCsvPayload(0, ROW_COUNT);
	}

	private static byte[] createCsvPayload(int startRow, int endRow) {
		StringBuilder csv = new StringBuilder((endRow - startRow) * CHANNEL_COUNT * 12);
		csv.append("time,seqNr");
		for (URI item : ITEMS) {
			csv.append(',').append(item).append('@').append(PROPERTY);
		}
		csv.append('\n');

		for (int row = startRow; row < endRow; row++) {
			csv.append(START_TIME + row / SEQUENCES_PER_TIMESTAMP)
					.append(',').append(row % SEQUENCES_PER_TIMESTAMP + 1);
			for (int channel = 0; channel < CHANNEL_COUNT; channel++) {
				csv.append(',').append(value(channel, row));
			}
			csv.append('\n');
		}
		return csv.toString().getBytes(StandardCharsets.UTF_8);
	}

	private static double value(int channel, int row) {
		return channel * 100_000.0 + row + 0.25;
	}
}