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
				var service = new AasFederatedService(url.replaceFirst("^aas-api:", ""));
				return service;
			}
		});

		sailRepository.init();
		this.repository = sailRepository;
	}

	@Test
	public void shellsTest() {
		try (RepositoryConnection conn = repository.getConnection()) {
			String query = "prefix aas: <https://admin-shell.io/aas/3/0/> " +
					"select distinct ?idShort where { " +
					"service <aas-api:https://v3.admin-shell-io.com> { " +
					"{ select ?shell { <aas-api:endpoint> <aas-api:shells> ?shell } limit 2 } " +
					"?shell aas:submodels ?list . ?list !<:> ?sm . ?sm (!<:>)+ ?element . " +
					"{ ?element a aas:Property } union { ?element a aas:MultiLanguageProperty } " +
					"?element a ?type ; aas:idShort ?idShort . " +
					"} " +
					"} order by ?idShort";
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
			String query = "prefix aas: <https://admin-shell.io/aas/3/0/> " +
					"select (count(*) as ?cnt) where { " +
					"service <aas-api:https://v3.admin-shell-io.com> { " +
					"<aas-api:endpoint> <aas-api:submodels> ?sm . " + // ?sm (!<:>)* ?s . ?s ?p ?o " +
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
