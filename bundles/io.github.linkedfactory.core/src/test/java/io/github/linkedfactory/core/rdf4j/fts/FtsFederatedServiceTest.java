package io.github.linkedfactory.core.rdf4j.fts;

import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FtsFederatedServiceTest {
	@Test
	public void federatedKeywordSearchJoinsWithGraphData() {
		FtsSearchBackend backend = request -> List.of(
				new FtsSearchHit("urn:item1", 0.91, "snippet item1"),
				new FtsSearchHit("urn:item2", 0.12, "snippet item2"));

		SailRepository repository = new SailRepository(new MemoryStore());
		repository.setFederatedServiceResolver(new BaseFederatedServiceResolver() {
			@Override
			protected FederatedService createService(String serviceUrl) {
				if (serviceUrl.startsWith("fts:")) {
					return new FtsFederatedService(backend);
				}
				return null;
			}
		});
		repository.init();

		try (var connection = repository.getConnection()) {
			var vf = connection.getValueFactory();
			connection.add(vf.createIRI("urn:item1"), vf.createIRI("urn:weight"), vf.createLiteral(70));
			connection.add(vf.createIRI("urn:item2"), vf.createIRI("urn:weight"), vf.createLiteral(40));

			String query = """
					prefix fts: <fts:>
					prefix dtsc: <urn:>
					select ?iri ?score where {
					  service <fts:> {
					    ?iri fts:keywords "some keywords" ;
					         fts:score ?score .
					  }
					  ?iri dtsc:weight ?weight .
					  filter (?weight > 50)
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				Assert.assertTrue(result.hasNext());
				var binding = result.next();
				Assert.assertEquals("urn:item1", binding.getValue("iri").stringValue());
				Assert.assertEquals(0.91d, ((org.eclipse.rdf4j.model.Literal) binding.getValue("score")).doubleValue(), 1e-6);
				Assert.assertFalse(result.hasNext());
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void supportsVariableKeywordAndLimit() {
		FtsSearchBackend backend = request -> {
			Assert.assertEquals("find me", request.getKeywords());
			Assert.assertEquals(1, request.getLimit());
			return List.of(new FtsSearchHit("urn:item1", null, null));
		};

		SailRepository repository = new SailRepository(new MemoryStore());
		repository.setFederatedServiceResolver(new BaseFederatedServiceResolver() {
			@Override
			protected FederatedService createService(String serviceUrl) {
				if (serviceUrl.startsWith("fts:")) {
					return new FtsFederatedService(backend);
				}
				return null;
			}
		});
		repository.init();

		try (var connection = repository.getConnection()) {
			String query = """
					prefix fts: <fts:>
					select ?iri where {
					  bind("find me" as ?q)
					  bind(1 as ?limit)
					  service <fts:> {
					    ?iri fts:keywords ?q ;
					         fts:limit ?limit .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				Assert.assertTrue(result.hasNext());
				Assert.assertEquals("urn:item1", result.next().getValue("iri").stringValue());
				Assert.assertFalse(result.hasNext());
			}
		} finally {
			repository.shutDown();
		}
	}
}
