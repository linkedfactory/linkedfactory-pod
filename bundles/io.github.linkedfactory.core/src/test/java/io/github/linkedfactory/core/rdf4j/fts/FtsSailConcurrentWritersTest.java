package io.github.linkedfactory.core.rdf4j.fts;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FtsSailConcurrentWritersTest {
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();
	private HttpServer server;
	private AtomicInteger requests;

	@Before
	public void setUp() throws IOException {
		requests = new AtomicInteger();
		server = HttpServer.create(new InetSocketAddress(0), 0);
		server.createContext("/fts/bulk", this::handleRequest);
		server.start();
	}

	@After
	public void tearDown() {
		if (server != null) {
			server.stop(0);
		}
	}

	@Test
	public void concurrentWritersCommitWithoutErrors() throws Exception {
		int threads = 4;
		int iterations = 25;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		List<Future<Void>> futures = new ArrayList<>();

		try {
			for (int t = 0; t < threads; t++) {
				final int threadIndex = t;
				Callable<Void> task = () -> {
					start.await(30, TimeUnit.SECONDS);
					Path outboxDir = Files.createTempDirectory("fts-outbox-soak-" + threadIndex);
					HttpFtsSearchService searchService = new HttpFtsSearchService(
							"http://127.0.0.1:" + server.getAddress().getPort(),
							"/fts/bulk",
							true,
							outboxDir.toString());
					SailRepository repository = new SailRepository(new FtsSail(searchService, new MemoryStore()));
					repository.init();
					try {
						for (int i = 0; i < iterations; i++) {
							try (SailRepositoryConnection connection = repository.getConnection()) {
								connection.begin();
								connection.add(vf.createIRI("urn:s" + threadIndex + ":" + i),
										vf.createIRI("urn:p"),
										vf.createLiteral("value-" + threadIndex + "-" + i));
								connection.commit();
							}
						}
						try (var files = Files.list(outboxDir)) {
							assertTrue(files.findAny().isEmpty());
						}
					} finally {
						repository.shutDown();
						searchService.shutdown();
					}
					return null;
				};
				futures.add(pool.submit(task));
			}

			start.countDown();
			for (Future<Void> future : futures) {
				future.get(2, TimeUnit.MINUTES);
			}

			assertEquals(threads * iterations, requests.get());
		} finally {
			pool.shutdownNow();
		}
	}

	private void handleRequest(HttpExchange exchange) throws IOException {
		requests.incrementAndGet();
		try (InputStream in = exchange.getRequestBody()) {
			in.readAllBytes();
		}
		byte[] response = "ok".getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(200, response.length);
		exchange.getResponseBody().write(response);
		exchange.close();
	}
}
