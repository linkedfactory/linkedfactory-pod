package io.github.linkedfactory.service.benchmark;

import org.junit.Test;
import org.mockito.MockedConstruction;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;

public class KvinIngestionBenchmarkTest {
	@Test
	public void mainRunsTheIngestionBenchmark() throws Exception {
		try (MockedConstruction<Runner> runners = mockConstruction(Runner.class, (runner, context) -> {
			Options options = (Options) context.arguments().get(0);
			assertEquals(List.of(KvinIngestionBenchmark.class.getSimpleName()), options.getIncludes());
		})) {
			KvinIngestionBenchmark.main(new String[0]);

			assertEquals(1, runners.constructed().size());
			verify(runners.constructed().get(0)).run();
		}
	}
}
