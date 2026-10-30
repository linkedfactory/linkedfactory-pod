package io.github.linkedfactory.service.benchmark;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.util.ParquetFormatParser;
import io.github.linkedfactory.core.kvin.util.ParquetFormatWriter;
import net.enilink.komma.core.URIs;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Separates Parquet codec costs from HTTP routing and persistence. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
@Threads(1)
public class KvinParquetFormatBenchmark {
	@Param({"false", "true"})
	public boolean mixedTypes;
	private byte[] payload;
	private List<KvinTuple> tuples;

	@Setup(Level.Trial)
	public void setup() throws IOException {
		var workload = new KvinIngestionWorkload(0, KvinIngestionWorkload.ROW_COUNT, mixedTypes);
		payload = workload.parquetPayload();
		tuples = workload.tuples();
		try (var reader = parser().parse()) {
			if (!tuples.equals(reader.toList())) throw new IllegalStateException("Parquet round trip failed");
		}
	}

	private ParquetFormatParser parser() {
		return new ParquetFormatParser(URIs.createURI("http://foo.com/linkedfactory/"), payload);
	}

	@Benchmark
	@OperationsPerInvocation(KvinIngestionWorkload.TUPLE_COUNT)
	public void parse(Blackhole bh) throws IOException {
		try (var reader = parser().parse()) {
			while (reader.hasNext()) bh.consume(reader.next());
		}
	}

	@Benchmark
	@OperationsPerInvocation(KvinIngestionWorkload.TUPLE_COUNT)
	public void write() throws IOException {
		try (var writer = new ParquetFormatWriter(OutputStream.nullOutputStream())) {
			for (KvinTuple tuple : tuples) writer.writeTuple(tuple);
		}
	}
}
