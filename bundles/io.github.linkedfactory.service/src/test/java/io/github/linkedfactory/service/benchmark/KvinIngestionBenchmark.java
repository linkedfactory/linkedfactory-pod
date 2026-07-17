package io.github.linkedfactory.service.benchmark;

import com.google.inject.Guice;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.util.CsvFormatParser;
import io.github.linkedfactory.service.KvinService;
import io.github.linkedfactory.service.MockHttpServletRequest;
import io.github.linkedfactory.service.util.JsonFormatParser$;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.KommaModule;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import net.enilink.komma.model.IModelSet;
import net.enilink.komma.model.IModelSetFactory;
import net.enilink.komma.model.MODELS;
import net.enilink.komma.model.ModelPlugin;
import net.enilink.komma.model.ModelSetModule;
import net.enilink.platform.lift.util.Globals;
import net.liftweb.common.Box;
import net.liftweb.common.Full;
import net.liftweb.http.CurrentReq$;
import net.liftweb.http.LiftResponse;
import net.liftweb.http.Req;
import net.liftweb.http.provider.servlet.HTTPRequestServlet;
import net.liftweb.util.VendorJ;
import org.json4s.JValue;
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
import org.junit.Assert;
import scala.Function0;
import scala.PartialFunction;
import scala.collection.immutable.Nil$;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
@Threads(1)
public class KvinIngestionBenchmark {
	private static final int SEQUENTIAL_CSV_FILE_COUNT = 10;

	@State(Scope.Thread)
	public static class BenchmarkState {
		private KvinIngestionWorkload workload;
		private IModelSet modelSet;
		private KvinLevelDb store;
		private File storeDirectory;
		private KvinService service;
		private KvinService parseOnlyService;
		private byte[] jsonPayload;
		private byte[] csvPayload;
		private List<byte[]> csvPayloads;
		private boolean measuredWrites;

		@Setup(Level.Trial)
		public void setupTrial() {
			workload = new KvinIngestionWorkload();
			jsonPayload = workload.jsonPayload();
			csvPayload = workload.csvPayload();
			csvPayloads = workload.csvPayloads(SEQUENTIAL_CSV_FILE_COUNT);
			try {
				KommaModule module = ModelPlugin.createModelSetModule(
						Class.forName("net.enilink.komma.model.ModelPlugin").getClassLoader());
				IModelSetFactory factory = (IModelSetFactory) Guice.createInjector(new ModelSetModule(module))
						.getInstance(Class.forName("net.enilink.komma.model.IModelSetFactory"));
				modelSet = factory.createModelSet(MODELS.NAMESPACE_URI.appendFragment("MemoryModelSet"));
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException("Could not initialize the Komma model set", e);
			}
			Globals.contextModelSet().theDefault().set(VendorJ.vendor(new Full(modelSet)));
		}

		@Setup(Level.Invocation)
		public void setupInvocation() throws IOException {
			String tempRoot = System.getProperty("jmh.temp.root", "");
			Path directory = tempRoot.isEmpty()
					? Files.createTempDirectory("kvin-ingestion-jmh-")
					: Files.createTempDirectory(Path.of(tempRoot), "kvin-ingestion-jmh-");
			storeDirectory = directory.toFile();
			store = new KvinLevelDb(storeDirectory);
			store.put(workload.preseedTuples());
			service = createService();
			parseOnlyService = createParseOnlyService();
			measuredWrites = false;
		}

		@TearDown(Level.Invocation)
		public void teardownInvocation() throws IOException {
			try {
				validateStore();
			} finally {
				if (store != null) {
					store.close();
					store = null;
				}
				if (storeDirectory != null) {
					deleteDirectory(storeDirectory.toPath());
					storeDirectory = null;
				}
			}
		}

		@TearDown(Level.Trial)
		public void teardownTrial() {
			if (modelSet != null) {
				modelSet.dispose();
				modelSet = null;
			}
		}

		public void putBatch() {
			store.put(workload.tuples());
			measuredWrites = true;
		}

		public void putScalar() {
			for (KvinTuple tuple : workload.tuples()) {
				store.put(tuple);
			}
			measuredWrites = true;
		}

		public void postJson() throws IOException {
			post(jsonPayload, "application/json", service, true);
		}

		public void postJsonParseOnly() throws IOException {
			post(jsonPayload, "application/json", parseOnlyService, false);
		}

		public void postCsv() throws IOException {
			post(csvPayload, "text/csv", service, true);
		}

		public void putCsvDirect() throws IOException {
			CsvFormatParser parser = new CsvFormatParser(
					URIs.createURI("http://foo.com/linkedfactory/"), ',',
					new ByteArrayInputStream(csvPayload));
			parser.setContext(KvinIngestionWorkload.CONTEXT);
			IExtendedIterator<KvinTuple> tuples = parser.parse();
			try {
				store.put(tuples);
			} finally {
				tuples.close();
			}
			measuredWrites = true;
		}

		public void postCsvSequentialFiles() throws IOException {
			for (byte[] payload : csvPayloads) {
				post(payload, "text/csv", service, true);
			}
		}

		private void post(byte[] payload, String contentType, KvinService targetService,
				boolean writesMeasuredTuples) throws IOException {
			MockHttpServletRequest request = new MockHttpServletRequest("http://foo.com/linkedfactory/values");
			request.method_$eq("POST");
			request.body_$eq(payload);
			request.contentType_$eq(contentType);
			Req req = Req.apply(new HTTPRequestServlet(request, null),
					Nil$.MODULE$.$colon$colon(PartialFunction.empty()), System.nanoTime());
			Box<LiftResponse> result = targetService.apply(req).apply();
			LiftResponse response = result.openOr(null);
			if (response == null || response.toResponse().code() != 200) {
				int status = response == null ? -1 : response.toResponse().code();
				throw new IOException("KVIN ingestion request failed with HTTP status " + status);
			}
			if (writesMeasuredTuples) {
				measuredWrites = true;
			}
		}

		private KvinService createService() {
			return new BenchmarkService(false);
		}

		private KvinService createParseOnlyService() {
			return new BenchmarkService(true);
		}

		private class BenchmarkService extends KvinService {
			private final boolean parseOnly;

			private BenchmarkService(boolean parseOnly) {
				super(Nil$.MODULE$.$colon$colon("linkedfactory"), store);
				this.parseOnly = parseOnly;
			}

			@Override
			public URI contextModelUri() {
				return KvinIngestionWorkload.CONTEXT;
			}

			@Override
			public Box<?> saveValues(JValue json, scala.collection.immutable.List<String> path, long currentTime) {
				if (!parseOnly) {
					return super.saveValues(json, path, currentTime);
				}
				return JsonFormatParser$.MODULE$.parseItem(URIs.createURI("http://foo.com/linkedfactory/"),
						contextModelUri(), json, currentTime);
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

		private void validateStore() {
			Set<KvinTuple> expected = new HashSet<>(workload.preseedTuples());
			if (measuredWrites) {
				expected.addAll(workload.tuples());
			}
			Set<KvinTuple> actual = new HashSet<>();
			for (URI item : KvinIngestionWorkload.ITEMS) {
				IExtendedIterator<KvinTuple> iterator = store.fetch(item, KvinIngestionWorkload.PROPERTY,
						KvinIngestionWorkload.CONTEXT, 0);
				try {
					while (iterator.hasNext()) {
						actual.add(iterator.next());
					}
				} finally {
					iterator.close();
				}
			}
			Assert.assertEquals("Unexpected persisted KVIN tuples", expected, actual);
			Assert.assertEquals((measuredWrites ? KvinIngestionWorkload.TUPLE_COUNT : 0)
					+ KvinIngestionWorkload.CHANNEL_COUNT, actual.size());
		}

		private static void deleteDirectory(Path directory) throws IOException {
			if (!Files.exists(directory)) {
				return;
			}
			Files.walkFileTree(directory, new SimpleFileVisitor<>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.deleteIfExists(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exception) throws IOException {
					Files.deleteIfExists(dir);
					return FileVisitResult.CONTINUE;
				}
			});
		}
	}

	@Benchmark
	public void putBatch(BenchmarkState state) {
		state.putBatch();
	}

	@Benchmark
	public void postJson(BenchmarkState state) throws IOException {
		state.postJson();
	}

	@Benchmark
	public void postCsv(BenchmarkState state) throws IOException {
		state.postCsv();
	}

	@Benchmark
	public void putCsvDirect(BenchmarkState state) throws IOException {
		state.putCsvDirect();
	}

	@Benchmark
	public void postCsvSequentialFiles(BenchmarkState state) throws IOException {
		state.postCsvSequentialFiles();
	}
}