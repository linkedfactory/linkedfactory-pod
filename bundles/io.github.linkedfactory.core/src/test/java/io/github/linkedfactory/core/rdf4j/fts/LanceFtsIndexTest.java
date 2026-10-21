package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LanceFtsIndexTest {
	private static final AtomicInteger COUNTER = new AtomicInteger();

	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();
	private LanceFtsIndex index;
	private LanceFtsSearchService searchService;
	private LanceFtsSearchBackend searchBackend;

	@Before
	public void setUp() {
		Assume.assumeFalse("org.lance:lance-core ships no Windows native library",
				System.getProperty("os.name", "").toLowerCase().contains("win"));
		index = new LanceFtsIndex("memory://lance-fts-" + COUNTER.incrementAndGet());
		searchService = new LanceFtsSearchService(index);
		searchBackend = new LanceFtsSearchBackend(index);
	}

	@After
	public void tearDown() throws Exception {
		if (index != null) {
			index.close();
		}
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
	public void updatingOneFieldKeepsOtherFieldsSearchable() throws Exception {
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
	public void limitIsHonored() throws Exception {
		IRI title = vf.createIRI("urn:title");
		Set<Statement> adds = new java.util.LinkedHashSet<>();
		for (int i = 0; i < 5; i++) {
			adds.add(vf.createStatement(vf.createIRI("urn:item:" + i), title, vf.createLiteral("fox number " + i)));
		}
		commit(adds, Set.of());

		List<FtsSearchHit> limited = searchBackend.search(new FtsSearchRequest("fox", null, 2, null, false, null));
		assertEquals(2, limited.size());

		List<FtsSearchHit> all = searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null));
		assertEquals(5, all.size());
	}

	@Test
	public void scoreAndSnippetAreNotSupportedByThisBackend() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		commit(Set.of(vf.createStatement(s1, title, vf.createLiteral("the quick brown fox"))), Set.of());

		List<FtsSearchHit> hits = searchBackend.search(new FtsSearchRequest("fox", null, 10, null, true, null));
		assertEquals(1, hits.size());
		assertNull("Lance Java FTS scan does not expose a relevance score", hits.get(0).getScore());
		assertNull("snippet/highlighting is not implemented for this backend", hits.get(0).getSnippet());
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
	public void quotesInLiteralValuesDoNotBreakRemoval() throws Exception {
		IRI s1 = vf.createIRI("urn:item:1");
		IRI title = vf.createIRI("urn:title");
		Statement stmt = vf.createStatement(s1, title, vf.createLiteral("it's a fox's tale"));

		commit(Set.of(stmt), Set.of());
		assertEquals(1, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());

		commit(Set.of(), Set.of(stmt));
		assertEquals(0, searchBackend.search(new FtsSearchRequest("fox", null, 10, null, false, null)).size());
	}

	private void commit(Set<Statement> added, Set<Statement> removed) throws Exception {
		searchService.begin();
		searchService.addRemoveStatements(added, removed);
		searchService.commit();
	}
}
