package io.github.linkedfactory.core.rdf4j.aas;

import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Ignore
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

		sailRepository.setFederatedServiceResolver(new BaseFederatedServiceResolver() {
			@Override
			public FederatedService createService(String url) {
				var service = new AasFederatedService(url.replaceFirst("^aas-api:", ""), this::getExecutorService);
				return service;
			}
		});

		sailRepository.init();

		// the following data influences the query optimizer somehow
		try (RepositoryConnection connection = sailRepository.getConnection()) {
			connection.add(getClass().getResource("/META-INF/ontologies/rdfs.rdf"), RDFFormat.RDFXML);
		} catch (IOException e) {
			e.printStackTrace();
		}

		this.repository = sailRepository;
	}

	@Test
	public void shellsTest() {
		try (RepositoryConnection conn = repository.getConnection()) {
			String query = "prefix aas: <https://admin-shell.io/aas/3/0/> " +
					"select distinct ?idShort where { " +
					"service <aas-api:https://v3.admin-shell-io.com> { " +
					"{ select ?shell { <aas-api:endpoint> <aas-api:shells> ?shell } limit 2 } " +
					"?shell aas:submodels ?sm . ?sm (!<:>)+ ?element . " +
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
	public void findNameplateTest() {
		try (RepositoryConnection conn = repository.getConnection()) {
			String query = "prefix aas: <https://admin-shell.io/aas/3/0/> " +
					"select ?sm ?element ?p ?o { " +
					"service <aas-api:https://v3.admin-shell-io.com> { " +
					"{ select ?shell { <aas-api:endpoint> <aas-api:shells> ?shell } limit 2 } " +
					"?shell aas:submodels ?sm . ?sm aas:semanticId ?semId . ?semId aas:keys ?key . " +
					"?key aas:value \"https://admin-shell.io/zvei/nameplate/1/0/Nameplate\" . " +
					"?sm !<:> ?element . " +
					"{ ?element a aas:Property } union { ?element a aas:MultiLanguageProperty } " +
					"?element ?p ?o . " +
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
	public void copyShellTest() {
		try (RepositoryConnection conn = repository.getConnection()) {
			String query = "prefix aas: <https://admin-shell.io/aas/3/0/> " +
					"insert { " +
					"?s ?p ?o . " +
					"} where { " +
					"service <aas-api:https://v3.admin-shell-io.com> { " +
					"{ select ?shell { <aas-api:endpoint> <aas-api:shells> ?shell } limit 2 } " +
					"?shell aas:submodels ?sm . ?sm (!<:>)* ?s . ?s ?p ?o " +
					"} " +
					"}";
			conn.prepareUpdate(query).execute();
			conn.getStatements(null, null, null).stream().forEach(stmt -> {
				System.out.println(stmt);
			});
		}
	}

	@Test
	public void submodelsTest() {
		try (RepositoryConnection conn = repository.getConnection()) {
			String query = "prefix aas: <https://admin-shell.io/aas/3/0/> " +
					"select ?sm where { " +
					"service <aas-api:https://v3.admin-shell-io.com> { " +
					"<aas-api:endpoint> <aas-api:submodels> ?sm ." +
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
