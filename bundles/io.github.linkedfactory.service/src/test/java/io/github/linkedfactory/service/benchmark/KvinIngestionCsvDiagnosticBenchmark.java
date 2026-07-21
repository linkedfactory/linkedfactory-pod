package io.github.linkedfactory.service.benchmark;

import com.google.inject.Guice;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import io.github.linkedfactory.core.kvin.DelegatingKvin;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.util.CsvFormatParser;
import io.github.linkedfactory.service.KvinService;
import io.github.linkedfactory.service.MockHttpServletRequest;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.KommaModule;
import net.enilink.komma.core.URI;
import net.enilink.komma.model.IModelSet;
import net.enilink.komma.model.IModelSetFactory;
import net.enilink.komma.model.MODELS;
import net.enilink.komma.model.ModelPlugin;
import net.enilink.komma.model.ModelSetModule;
import net.enilink.platform.lift.util.Globals;
import net.liftweb.common.Box;
import net.liftweb.http.CurrentReq$;
import net.liftweb.http.LiftResponse;
import net.liftweb.http.Req;
import net.liftweb.http.provider.servlet.HTTPRequestServlet;
import net.liftweb.util.VendorJ;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import scala.Function0;
import scala.PartialFunction;
import scala.collection.immutable.Nil$;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
@Threads(1)
public class KvinIngestionCsvDiagnosticBenchmark {
	@State(Scope.Thread)
	public static class BenchmarkState {
		private static final int EXPECTED_FIELD_COUNT = KvinIngestionWorkload.ROW_COUNT + 1;
		private static final int EXPECTED_FIELDS_PER_ROW = KvinIngestionWorkload.CHANNEL_COUNT + 2;

		private KvinIngestionWorkload workload;
		private byte[] csvPayload;
		private IModelSet modelSet;
		private ConsumingKvin sink;
		private KvinService service;
		private String measuredStage;
		private int rowCount;
		private int fieldCount;
		private int tupleCount;

		@Setup(Level.Trial)
		public void setupTrial() {
			workload = new KvinIngestionWorkload();
			csvPayload = workload.csvPayload();

			KommaModule module = ModelPlugin.createModelSetModule(ModelPlugin.class.getClassLoader());
			IModelSetFactory factory =
					Guice.createInjector(new ModelSetModule(module))
							.getInstance(IModelSetFactory.class);
			modelSet = factory.createModelSet(MODELS.NAMESPACE_URI.appendFragment("MemoryModelSet"));

			Globals.contextModelSet().theDefault().set(VendorJ.vendor(new net.liftweb.common.Full(modelSet)));
			sink = new ConsumingKvin();
			service = new BenchmarkService(sink);
		}

		@TearDown(Level.Invocation)
		public void validateInvocation() {
			switch (measuredStage) {
			case "consumePrebuilt", "parseCsvAndConsumeTuples", "postCsvParseOnly" ->
					org.junit.Assert.assertEquals(KvinIngestionWorkload.TUPLE_COUNT, tupleCount);
			case "decodeCsvAndConsumeFields" -> {
				org.junit.Assert.assertEquals(EXPECTED_FIELD_COUNT, rowCount);
				org.junit.Assert.assertEquals(EXPECTED_FIELD_COUNT * EXPECTED_FIELDS_PER_ROW, fieldCount);
			}
			default -> throw new AssertionError("Unknown CSV diagnostic stage " + measuredStage);
		}
		}

		@TearDown(Level.Trial)
		public void teardownTrial() {
			if (modelSet != null) {
				modelSet.dispose();
				modelSet = null;
			}
		}

		public void consumePrebuilt(Blackhole blackhole) {
			measuredStage = "consumePrebuilt";
			tupleCount = 0;
			for (KvinTuple tuple : workload.tuples()) {
				blackhole.consume(tuple);
				tupleCount++;
			}
		}

		public void decodeCsvAndConsumeFields(Blackhole blackhole) throws IOException {
			measuredStage = "decodeCsvAndConsumeFields";
			rowCount = 0;
			fieldCount = 0;
			CSVParser parser = new CSVParserBuilder()
					.withSeparator(',')
					.withIgnoreQuotations(true)
					.build();
			try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(
					new ByteArrayInputStream(csvPayload), StandardCharsets.UTF_8))
					.withSkipLines(0)
					.withCSVParser(parser)
					.build()) {
				String[] row;
				while ((row = readNext(reader)) != null) {
					rowCount++;
					for (String field : row) {
						blackhole.consume(field);
						fieldCount++;
					}
				}
			}
		}

		public void parseCsvAndConsumeTuples(Blackhole blackhole) throws IOException {
			measuredStage = "parseCsvAndConsumeTuples";
			tupleCount = 0;
			CsvFormatParser parser = new CsvFormatParser(URIsForBenchmark.BASE, ',',
					new ByteArrayInputStream(csvPayload));
			parser.setContext(KvinIngestionWorkload.CONTEXT);
			IExtendedIterator<KvinTuple> tuples = parser.parse();
			try {
				while (tuples.hasNext()) {
					blackhole.consume(tuples.next());
					tupleCount++;
				}
			} finally {
				tuples.close();
			}
		}

		public void postCsvParseOnly(Blackhole blackhole) throws IOException {
			measuredStage = "postCsvParseOnly";
			tupleCount = 0;
			sink.startInvocation(blackhole);
			MockHttpServletRequest request = new MockHttpServletRequest("http://foo.com/linkedfactory/values");
			request.method_$eq("POST");
			request.body_$eq(csvPayload);
			request.contentType_$eq("text/csv");
			Req req = Req.apply(new HTTPRequestServlet(request, null),
					Nil$.MODULE$.$colon$colon(PartialFunction.empty()), System.nanoTime());
			Box<LiftResponse> result = service.apply(req).apply();
			LiftResponse response = result.openOr(null);
			if (response == null || response.toResponse().code() != 200) {
				int status = response == null ? -1 : response.toResponse().code();
				throw new IOException("CSV parse-only request failed with HTTP status " + status);
			}
			tupleCount = sink.tupleCount();
		}

		private static String[] readNext(CSVReader reader) throws IOException {
			try {
				return reader.readNext();
			} catch (CsvValidationException e) {
				throw new IOException(e);
			}
		}

		private class BenchmarkService extends KvinService {
			private BenchmarkService(Kvin sink) {
				super(Nil$.MODULE$.$colon$colon("linkedfactory"), sink);
			}

			@Override
			public URI contextModelUri() {
				return KvinIngestionWorkload.CONTEXT;
			}

			@Override
			public Function0<Box<LiftResponse>> apply(Req in) {
				IModelSet currentModelSet = Globals.contextModelSet().vend().openOr(null);
				return CurrentReq$.MODULE$.doWith(in, () -> {
					try {
						currentModelSet.getUnitOfWork().begin();
						if (isDefinedAt(in)) {
							return super.apply(in);
						}
						return () -> Box.legacyNullTest((LiftResponse) null);
					} finally {
						currentModelSet.getUnitOfWork().end();
					}
				});
			}
		}
	}

	static class ConsumingKvin extends DelegatingKvin {
		private Blackhole blackhole;
		private int iterablePutCount;
		private int scalarPutCount;
		private int tupleCount;

		ConsumingKvin() {
			super(() -> null);
		}

		void startInvocation(Blackhole blackhole) {
			this.blackhole = blackhole;
			iterablePutCount = 0;
			scalarPutCount = 0;
			tupleCount = 0;
		}

		@Override
		public void put(KvinTuple... tuples) {
			scalarPutCount++;
			throw new IllegalStateException("CSV ingestion used scalar KVIN persistence");
		}

		@Override
		public void put(Iterable<KvinTuple> tuples) {
			iterablePutCount++;
			for (KvinTuple tuple : tuples) {
				consumeTuple(tuple);
				tupleCount++;
			}
		}

		protected void consumeTuple(KvinTuple tuple) {
			blackhole.consume(tuple);
		}

		int iterablePutCount() {
			return iterablePutCount;
		}

		int scalarPutCount() {
			return scalarPutCount;
		}

		int tupleCount() {
			return tupleCount;
		}
	}

	private static final class URIsForBenchmark {
		private static final URI BASE = net.enilink.komma.core.URIs.createURI("http://foo.com/linkedfactory/");
	}

	@Benchmark
	public void consumePrebuilt(BenchmarkState state, Blackhole blackhole) {
		state.consumePrebuilt(blackhole);
	}

	@Benchmark
	public void decodeCsvAndConsumeFields(BenchmarkState state, Blackhole blackhole) throws IOException {
		state.decodeCsvAndConsumeFields(blackhole);
	}

	@Benchmark
	public void parseCsvAndConsumeTuples(BenchmarkState state, Blackhole blackhole) throws IOException {
		state.parseCsvAndConsumeTuples(blackhole);
	}

	@Benchmark
	public void postCsvParseOnly(BenchmarkState state, Blackhole blackhole) throws IOException {
		state.postCsvParseOnly(blackhole);
	}
}