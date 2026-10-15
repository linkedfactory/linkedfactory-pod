package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LuceneFtsIndexTest {
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();
	private Path indexDir;
	private LuceneFtsIndex index;
	private LuceneFtsSearchService searchService;
	private LuceneFtsSearchBackend searchBackend;

	@Before
	public void setUp() throws IOException {
		indexDir = Files.createTempDirectory("lucene-fts-");
		index = new LuceneFtsIndex(indexDir);
		searchService = new LuceneFtsSearchService(index);
		searchBackend = new LuceneFtsSearchBackend(index);
	}

	@After
	public void tearDown() throws Exception {
		index.close();
		deleteRecursively(indexDir);
	}

	@Test
	public void indexesLiteralAndFindsItByKeyword() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		Statement stmt = vf.createStatement(s1, title, vf.createLiteral("The quick brown fox"));

		commit(Set.of(stmt), Set.of());

		List<FtsSearchHit> hits = searchBackend.search(new FtsSearchRequest("quick", null, 10, null, false, null));
		assertEquals(1, hits.size());
		assertEquals("urn:item:1", hits.get(0).getIri());
	}

	@Test
	public void searchOnSpecificFieldOnlyMatchesThatPredicate() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		IRI body = vf.createIRI("urn:body");
		commit(Set.of(
				vf.createStatement(s1, title, vf.createLiteral("fox")),
				vf.createStatement(s1, body, vf.createLiteral("unrelated text"))
		), Set.of());

		List<FtsSearchHit> onTitle = searchBackend.search(new FtsSearchRequest("fox", "urn:title", 10, null, false, null));
		assertEquals(1, onTitle.size());

		List<FtsSearchHit> onBody = searchBackend.search(new FtsSearchRequest("fox", "urn:body", 10, null, false, null));
		assertEquals(0, onBody.size());
	}

	@Test
	public void removingStatementDropsItFromResults() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		Statement stmt = vf.createStatement(s1, title, vf.createLiteral("fox"));

		commit(Set.of(stmt), Set.of());
		assertEquals(1, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());

		commit(Set.of(), Set.of(stmt));
		assertEquals(0, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());
	}

	@Test
	public void updatingOneFieldKeepsOtherFieldsOnSameDocument() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		IRI body = vf.createIRI("urn:body");
		Statement titleStmt = vf.createStatement(s1, title, vf.createLiteral("fox"));
		Statement bodyStmt = vf.createStatement(s1, body, vf.createLiteral("hound"));
		commit(Set.of(titleStmt, bodyStmt), Set.of());

		commit(Set.of(), Set.of(titleStmt));

		assertEquals(0, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());
		assertEquals(1, searchBackend.search(new FtsSearchRequest("hound", null, 10, null, false, null)).size());
	}

	@Test
	public void clearWipesEverything() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		commit(Set.of(vf.createStatement(s1, title, vf.createLiteral("fox"))), Set.of());
		assertEquals(1, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());

		searchService.begin();
		searchService.clear();
		searchService.commit();

		assertEquals(0, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());
	}

	@Test
	public void clearThenAddInSameTransactionKeepsTheReAddedStatement() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		commit(Set.of(vf.createStatement(s1, title, vf.createLiteral("fox"))), Set.of());

		searchService.begin();
		searchService.clear();
		searchService.addRemoveStatements(Set.of(vf.createStatement(s1, title, vf.createLiteral("fox"))), Set.of());
		searchService.commit();

		assertEquals(1, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());
	}

	@Test
	public void clearContextsOnlyRemovesMatchingGraph() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI s2 = vf.createIRI("urn:item:2");
		IRI title = vf.createIRI("urn:title");
		IRI ctxA = vf.createIRI("urn:ctx:a");
		IRI ctxB = vf.createIRI("urn:ctx:b");

		commit(Set.of(
				vf.createStatement(s1, title, vf.createLiteral("fox"), ctxA),
				vf.createStatement(s2, title, vf.createLiteral("fox"), ctxB)
		), Set.of());
		assertEquals(2, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());

		searchService.begin();
		searchService.clearContexts(ctxA);
		searchService.commit();

		List<FtsSearchHit> remaining = searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null));
		assertEquals(1, remaining.size());
		assertEquals("urn:item:2", remaining.get(0).getIri());
	}

	@Test
	public void boostAndLimitAreHonored() throws Exception {
		IRI title = vf.createIRI("urn:title");
		Set<Statement> adds = new java.util.LinkedHashSet<>();
		for (int i = 0; i < 5; i++) {
			adds.add(vf.createStatement(vf.createIRI("urn:item:" + i), title, vf.createLiteral("fox number " + i)));
		}
		commit(adds, Set.of());

		List<FtsSearchHit> limited = searchBackend.search(new FtsSearchRequest("fox", null, 2, null, false, null));
		assertEquals(2, limited.size());

		List<FtsSearchHit> boosted = searchBackend.search(new FtsSearchRequest("fox", null, 10, 5.0, false, null));
		assertEquals(5, boosted.size());
		assertTrue(boosted.get(0).getScore() > 0);
	}

	@Test
	public void snippetIsReturnedWhenRequested() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		commit(Set.of(vf.createStatement(s1, title, vf.createLiteral("the quick brown fox jumps"))), Set.of());

		List<FtsSearchHit> hits = searchBackend.search(new FtsSearchRequest("fox", null, 10, null, true, null));
		assertEquals(1, hits.size());
		assertTrue(hits.get(0).getSnippet() != null && hits.get(0).getSnippet().contains("fox"));

		List<FtsSearchHit> withoutSnippet = searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null));
		assertNull(withoutSnippet.get(0).getSnippet());
	}

	@Test
	public void iriFilterRestrictsToOneSubject() throws Exception {
		IRI title = vf.createIRI("urn:title");
		commit(Set.of(
				vf.createStatement(vf.createIRI("urn:item:1"), title, vf.createLiteral("fox")),
				vf.createStatement(vf.createIRI("urn:item:2"), title, vf.createLiteral("fox"))
		), Set.of());

		List<FtsSearchHit> filtered = searchBackend.search(
				new FtsSearchRequest("fox", null, 10, null, false, "urn:item:2"));
		assertEquals(1, filtered.size());
		assertEquals("urn:item:2", filtered.get(0).getIri());
	}

	@Test
	public void searchToleratesLuceneSpecialCharactersInsteadOfThrowing() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		commit(Set.of(vf.createStatement(s1, title, vf.createLiteral("fox"))), Set.of());

		List<FtsSearchHit> hits = searchBackend.search(
				new FtsSearchRequest("fox AND (unterminated: / + - [ ] \" ", null, 10, null, false, null));
		assertTrue(hits.size() >= 0);
	}

	@Test
	public void concurrentCommitsOnSharedServiceDoNotLoseStatements() throws Exception {
		int threads = 4;
		int perThread = 50;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		List<Future<Void>> futures = new java.util.ArrayList<>();
		IRI title = vf.createIRI("urn:title");

		try {
			for (int t = 0; t < threads; t++) {
				final int threadIndex = t;
				futures.add(pool.submit(() -> {
					start.await(30, TimeUnit.SECONDS);
					for (int i = 0; i < perThread; i++) {
						IRI subject = vf.createIRI("urn:item:" + threadIndex + ":" + i);
						commit(Set.of(vf.createStatement(subject, title, vf.createLiteral("fox"))), Set.of());
					}
					return null;
				}));
			}
			start.countDown();
			for (Future<Void> future : futures) {
				future.get(2, TimeUnit.MINUTES);
			}
		} finally {
			pool.shutdownNow();
		}

		List<FtsSearchHit> hits = searchBackend.search(new FtsSearchRequest("fox", null, threads * perThread + 1, null, false, null));
		assertEquals(threads * perThread, hits.size());
	}

	private void commit(Set<Statement> added, Set<Statement> removed) throws Exception {
		searchService.begin();
		searchService.addRemoveStatements(added, removed);
		searchService.commit();
	}

	private static void deleteRecursively(Path dir) throws IOException {
		if (!Files.exists(dir)) {
			return;
		}
		try (var stream = Files.walk(dir)) {
			stream.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
				try {
					Files.deleteIfExists(p);
				} catch (IOException ignored) {
				}
			});
		}
	}
}
