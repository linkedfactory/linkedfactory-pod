package io.github.linkedfactory.core.rdf4j.vector;

import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JVectorIndexTest {
	private static final int DIMENSION = 4;

	private JVectorIndex index;
	private JVectorSearchService searchService;
	private JVectorSearchBackend searchBackend;

	@Before
	public void setUp() {
		index = new JVectorIndex(DIMENSION, VectorSimilarityFunction.EUCLIDEAN);
		searchService = new JVectorSearchService(index);
		searchBackend = new JVectorSearchBackend(index);
	}

	@After
	public void tearDown() throws IOException {
		index.close();
	}

	private void commitUpsert(String iri, float[] vector) throws Exception {
		searchService.begin();
		searchService.upsert(iri, vector);
		searchService.commit();
	}

	@Test
	public void upsertAndSearchFindsNearestVector() throws Exception {
		commitUpsert("urn:item:1", new float[]{0, 0, 0, 0});
		commitUpsert("urn:item:2", new float[]{10, 10, 10, 10});

		List<VectorSearchHit> hits = searchBackend.search(new VectorSearchRequest(new float[]{1, 0, 0, 0}, 1, null));
		assertEquals(1, hits.size());
		assertEquals("urn:item:1", hits.get(0).getIri());
	}

	@Test
	public void removingVectorDropsItFromResults() throws Exception {
		commitUpsert("urn:item:1", new float[]{0, 0, 0, 0});
		commitUpsert("urn:item:2", new float[]{10, 10, 10, 10});

		searchService.begin();
		searchService.remove("urn:item:1");
		searchService.commit();

		List<VectorSearchHit> hits = searchBackend.search(new VectorSearchRequest(new float[]{0, 0, 0, 0}, 10, null));
		assertEquals(1, hits.size());
		assertEquals("urn:item:2", hits.get(0).getIri());
	}

	@Test
	public void updatingVectorOverwritesPreviousOneForSameSubject() throws Exception {
		commitUpsert("urn:item:1", new float[]{0, 0, 0, 0});
		commitUpsert("urn:item:1", new float[]{10, 10, 10, 10});

		assertEquals(1, index.size());
		List<VectorSearchHit> hits = searchBackend.search(new VectorSearchRequest(new float[]{10, 10, 10, 10}, 1, null));
		assertEquals(1, hits.size());
		assertEquals("urn:item:1", hits.get(0).getIri());
		assertTrue(hits.get(0).getScore() > 0.99);
	}

	@Test
	public void clearWipesTheWholeIndex() throws Exception {
		commitUpsert("urn:item:1", new float[]{0, 0, 0, 0});
		commitUpsert("urn:item:2", new float[]{10, 10, 10, 10});

		searchService.begin();
		searchService.clear();
		searchService.commit();

		assertEquals(0, index.size());
		List<VectorSearchHit> hits = searchBackend.search(new VectorSearchRequest(new float[]{0, 0, 0, 0}, 10, null));
		assertEquals(0, hits.size());
	}

	@Test
	public void rollbackDiscardsPendingChanges() throws Exception {
		commitUpsert("urn:item:1", new float[]{0, 0, 0, 0});

		searchService.begin();
		searchService.upsert("urn:item:2", new float[]{10, 10, 10, 10});
		searchService.rollback();

		assertEquals(1, index.size());
	}

	@Test
	public void topKLimitIsHonored() throws Exception {
		for (int i = 0; i < 5; i++) {
			commitUpsert("urn:item:" + i, new float[]{i, i, i, i});
		}

		List<VectorSearchHit> hits = searchBackend.search(new VectorSearchRequest(new float[]{0, 0, 0, 0}, 2, null));
		assertEquals(2, hits.size());
	}

	@Test
	public void iriFilterRestrictsCandidatesToThatSubject() throws Exception {
		commitUpsert("urn:item:close", new float[]{0, 0, 0, 0});
		commitUpsert("urn:item:far", new float[]{10, 10, 10, 10});

		List<VectorSearchHit> hits = searchBackend.search(
				new VectorSearchRequest(new float[]{0, 0, 0, 0}, 10, "urn:item:far"));
		assertEquals(1, hits.size());
		assertEquals("urn:item:far", hits.get(0).getIri());
	}

	@Test
	public void persistedGraphReloadsAndSearchesFromDisk() throws Exception {
		commitUpsert("urn:item:1", new float[]{0, 0, 0, 0});
		commitUpsert("urn:item:2", new float[]{10, 10, 10, 10});

		Path graphPath = Files.createTempFile("jvector-graph-", null);
		try {
			index.save(graphPath);
			Map<Integer, String> nodeToIri = index.nodeToIriSnapshot();

			OnDiskGraphIndex onDisk = JVectorIndex.loadForSearch(graphPath);
			try {
				List<VectorSearchHit> hits = JVectorIndex.searchOnDisk(onDisk, VectorSimilarityFunction.EUCLIDEAN,
						nodeToIri, new VectorSearchRequest(new float[]{1, 0, 0, 0}, 1, null));
				assertEquals(1, hits.size());
				assertEquals("urn:item:1", hits.get(0).getIri());
			} finally {
				onDisk.close();
			}
		} finally {
			Files.deleteIfExists(graphPath);
		}
	}

	@Test
	public void concurrentCommitsFromMultipleThreadsDoNotLoseVectors() throws Exception {
		int threads = 4;
		int perThread = 25;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		CountDownLatch ready = new CountDownLatch(threads);
		CountDownLatch go = new CountDownLatch(1);
		List<Future<?>> futures = new java.util.ArrayList<>();
		for (int t = 0; t < threads; t++) {
			int threadId = t;
			futures.add(pool.submit(() -> {
				ready.countDown();
				try {
					go.await();
					for (int i = 0; i < perThread; i++) {
						commitUpsert("urn:thread:" + threadId + ":" + i, new float[]{threadId, i, 0, 0});
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}));
		}
		ready.await();
		go.countDown();
		for (Future<?> future : futures) {
			future.get(60, TimeUnit.SECONDS);
		}
		pool.shutdown();

		assertEquals(threads * perThread, index.size());
	}
}
