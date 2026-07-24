package io.github.linkedfactory.core.rdf4j.fts;

import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

	@Test
	public void invalidIriReturnedByBackendFailsQuery() {
		FtsSearchBackend backend = request -> List.of(new FtsSearchHit("not a valid iri", null, null));

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
					  service <fts:> {
					    ?iri fts:keywords "query" .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				result.hasNext();
				result.next();
				Assert.fail("Expected query evaluation to fail");
			} catch (org.eclipse.rdf4j.query.QueryEvaluationException expected) {
				Assert.assertTrue(expected.getMessage().contains("Invalid IRI returned by FTS backend"));
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void duplicateKeywordsFailQuery() {
		FtsSearchBackend backend = request -> List.of();

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
					  service <fts:> {
					    ?iri fts:keywords "one" ;
					         fts:keywords "two" .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				result.hasNext();
				result.next();
				Assert.fail("Expected query evaluation to fail");
			} catch (org.eclipse.rdf4j.query.QueryEvaluationException expected) {
				Assert.assertTrue(expected.getMessage().contains("exactly one fts:keywords"));
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void mismatchedSubjectsFailQuery() {
		FtsSearchBackend backend = request -> List.of();

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
					  service <fts:> {
					    ?iri fts:keywords "one" .
					    ?other fts:score ?score .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				result.hasNext();
				result.next();
				Assert.fail("Expected query evaluation to fail");
			} catch (org.eclipse.rdf4j.query.QueryEvaluationException expected) {
				Assert.assertTrue(expected.getMessage().contains("same subject"));
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void invalidLimitBindingFailsQuery() {
		FtsSearchBackend backend = request -> List.of();

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
					  bind("ten" as ?limit)
					  service <fts:> {
					    ?iri fts:keywords "query" ;
					         fts:limit ?limit .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				result.hasNext();
				result.next();
				Assert.fail("Expected query evaluation to fail");
			} catch (org.eclipse.rdf4j.query.QueryEvaluationException expected) {
				Assert.assertTrue(expected.getMessage().contains("Invalid fts:limit value"));
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void missingKeywordsFailQuery() {
		FtsSearchBackend backend = request -> List.of();
		SailRepository repository = repository(backend);

		try (var connection = repository.getConnection()) {
			String query = """
					prefix fts: <fts:>
					select ?iri where {
					  service <fts:> {
					    ?iri fts:score ?score .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				result.hasNext();
				result.next();
				Assert.fail("Expected query evaluation to fail");
			} catch (org.eclipse.rdf4j.query.QueryEvaluationException expected) {
				Assert.assertTrue(expected.getMessage().contains("must include ?iri fts:keywords"));
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void duplicateBoostPatternsFailQuery() {
		FtsSearchBackend backend = request -> List.of();
		SailRepository repository = repository(backend);

		try (var connection = repository.getConnection()) {
			String query = """
					prefix fts: <fts:>
					select ?iri where {
					  service <fts:> {
					    ?iri fts:keywords "one" ;
					         fts:boost 1.0 ;
					         fts:boost 2.0 .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				result.hasNext();
				result.next();
				Assert.fail("Expected query evaluation to fail");
			} catch (org.eclipse.rdf4j.query.QueryEvaluationException expected) {
				Assert.assertTrue(expected.getMessage().contains("duplicate fts:boost"));
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void blankKeywordBindingSkipsBackendCall() {
		AtomicInteger called = new AtomicInteger();
		FtsSearchBackend backend = request -> {
			called.incrementAndGet();
			return List.of(new FtsSearchHit("urn:item1", null, null));
		};
		SailRepository repository = repository(backend);

		try (var connection = repository.getConnection()) {
			String query = """
					prefix fts: <fts:>
					select ?iri where {
					  bind("" as ?q)
					  service <fts:> {
					    ?iri fts:keywords ?q .
					  }
					}
					""";
			var tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (var result = tupleQuery.evaluate()) {
				Assert.assertFalse(result.hasNext());
			}
			Assert.assertEquals(0, called.get());
		} finally {
			repository.shutDown();
		}
	}

	private SailRepository repository(FtsSearchBackend backend) {
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
		return repository;
	}
}
