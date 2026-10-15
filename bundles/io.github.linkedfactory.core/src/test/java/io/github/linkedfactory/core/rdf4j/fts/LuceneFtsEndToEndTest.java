package io.github.linkedfactory.core.rdf4j.fts;

import io.github.linkedfactory.core.rdf4j.fts.config.FtsSailConfig;
import io.github.linkedfactory.core.rdf4j.fts.config.FtsSailFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LuceneFtsEndToEndTest {
	private Path indexDir;
	private SailRepository repository;
	private FtsFederatedServiceResolver resolver;

	@Before
	public void setUp() throws IOException {
		indexDir = Files.createTempDirectory("lucene-fts-e2e-");
	}

	@After
	public void tearDown() {
		if (resolver != null) {
			resolver.shutDown();
		}
		if (repository != null) {
			repository.shutDown();
		}
	}

	@Test
	public void indexedThroughFtsSailIsSearchableThroughDefaultFederatedServiceResolver() throws Exception {
		FtsSailConfig config = new FtsSailConfig();
		config.setBackend("lucene");
		config.setIndexDir(indexDir.toString());

		FtsSail sail = (FtsSail) new FtsSailFactory().getSail(config);
		sail.setBaseSail(new MemoryStore());

		repository = new SailRepository(sail);
		resolver = new FtsFederatedServiceResolver(sail.getFederatedServiceConfig());
		repository.setFederatedServiceResolver(resolver);
		repository.init();

		try (var connection = repository.getConnection()) {
			var vf = connection.getValueFactory();
			connection.begin();
			connection.add(vf.createIRI("urn:item1"), vf.createIRI("urn:title"), vf.createLiteral("the quick brown fox"));
			connection.commit();
		}

		try (var connection = repository.getConnection()) {
			String query = """
					prefix fts: <fts:>
					select ?iri where {
					  service <fts:> {
					    ?iri fts:keywords "fox" .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				assertTrue(result.hasNext());
				assertEquals("urn:item1", result.next().getValue("iri").stringValue());
				assertFalse(result.hasNext());
			}
		}
	}

	@Test
	public void removedStatementIsNoLongerFoundAfterCommit() throws Exception {
		FtsSailConfig config = new FtsSailConfig();
		config.setBackend("lucene");
		config.setIndexDir(indexDir.toString());

		FtsSail sail = (FtsSail) new FtsSailFactory().getSail(config);
		sail.setBaseSail(new MemoryStore());

		repository = new SailRepository(sail);
		resolver = new FtsFederatedServiceResolver(sail.getFederatedServiceConfig());
		repository.setFederatedServiceResolver(resolver);
		repository.init();

		var title = repository.getValueFactory().createIRI("urn:title");
		var item = repository.getValueFactory().createIRI("urn:item1");
		var literal = repository.getValueFactory().createLiteral("the quick brown fox");

		try (var connection = repository.getConnection()) {
			connection.begin();
			connection.add(item, title, literal);
			connection.commit();

			connection.begin();
			connection.remove(item, title, literal);
			connection.commit();
		}

		try (var connection = repository.getConnection()) {
			String query = """
					prefix fts: <fts:>
					select ?iri where {
					  service <fts:> {
					    ?iri fts:keywords "fox" .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				assertFalse(result.hasNext());
			}
		}
	}
}
