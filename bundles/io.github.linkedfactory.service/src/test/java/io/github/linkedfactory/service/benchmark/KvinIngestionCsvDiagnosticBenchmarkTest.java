package io.github.linkedfactory.service.benchmark;

import io.github.linkedfactory.core.kvin.KvinTuple;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KvinIngestionCsvDiagnosticBenchmarkTest {
	@Test
	public void consumingSinkUsesTheIterableContract() {
		Set<KvinTuple> observed = new HashSet<>();
		KvinIngestionCsvDiagnosticBenchmark.ConsumingKvin sink =
				new KvinIngestionCsvDiagnosticBenchmark.ConsumingKvin() {
					@Override
					protected void consumeTuple(KvinTuple tuple) {
						observed.add(tuple);
					}
				};
		KvinIngestionWorkload workload = new KvinIngestionWorkload();

		sink.startInvocation(null);
		sink.put(workload.tuples());

		assertEquals(1, sink.iterablePutCount());
		assertEquals(0, sink.scalarPutCount());
		assertEquals(KvinIngestionWorkload.TUPLE_COUNT, sink.tupleCount());
		assertEquals(new HashSet<>(workload.tuples()), observed);
		assertTrue(observed.stream().allMatch(tuple -> tuple.context.equals(KvinIngestionWorkload.CONTEXT)));
	}
}