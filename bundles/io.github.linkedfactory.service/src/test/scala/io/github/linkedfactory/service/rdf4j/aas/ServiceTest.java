package io.github.linkedfactory.service.rdf4j.aas;

import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ServiceTest {
	private Repository repository;

	@Before
	public void init() {
		createRepository();
	}

	@After
	public void closeRepository() {
		repository.shutDown();
	}

	void createRepository() {
		var memoryStore = new MemoryStore();
		var sailRepository = new SailRepository(memoryStore);

		sailRepository.setFederatedServiceResolver(new AbstractFederatedServiceResolver() {
			@Override
			public FederatedService createService(String url) {
				var service = new AasFederatedService();
				return service;
			}
		});

		sailRepository.init();
		this.repository = sailRepository;
	}

	@Test
	public void shellsTest() {
		try (RepositoryConnection conn = repository.getConnection()) {
			String query = "select * where { " +
					"service <aas:> { " +
					"{ select ?shell { <https://v3.admin-shell-io.com> <aas:shells> ?shell } limit 1 } " +
					"?shell <r:submodels> ?list . ?list !<:> ?sm . ?sm ?p ?o " +
					"} " +
					"}";
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				while (result.hasNext()) {
					System.out.println(result.next());
				}
			}
		}
	}

	@Test
	public void submodelsTest() {
		try (RepositoryConnection conn = repository.getConnection()) {
			String query = "select (count(*) as ?cnt) where { " +
					"service <aas:> { " +
					"<https://v3.admin-shell-io.com> <aas:submodels> ?sm . " + // ?sm (!<:>)* ?s . ?s ?p ?o " +
					"} " +
					"}";
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				while (result.hasNext()) {
					System.out.println(result.next());
				}
			}
		}
	}
}
