package io.github.linkedfactory.core.rdf4j.aas;

import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for extraction data from AAS shells and submodels via SPARQL.
 */
public class AasServiceTest extends Mockito {
	static String toResponse(String json) {
		return String.format("{\"result\": [%s]}", json);
	}

	@Test
	public void testPropertyExtractionByIdShort() throws Exception {
		var mockedHttpClient = mock(CloseableHttpClient.class);
		var statusLine = mock(StatusLine.class);
		when(statusLine.getStatusCode()).thenReturn(200);

		var shellsResponse = mock(CloseableHttpResponse.class);
		when(shellsResponse.getStatusLine()).thenReturn(statusLine);
		when(shellsResponse.getEntity()).thenReturn(new StringEntity(
				toResponse(new String(getClass().getResourceAsStream("/aas/AAS_Motor_1_Type_shell.json").readAllBytes())))
		);
		when(mockedHttpClient.execute(argThat(httpUriRequest ->
				httpUriRequest != null && httpUriRequest.getURI().toString().contains("/shells")))).thenReturn(shellsResponse);
		var submodelResponse = mock(CloseableHttpResponse.class);
		when(submodelResponse.getStatusLine()).thenReturn(statusLine);
		when(submodelResponse.getEntity()).thenReturn(new StringEntity(
				new String(getClass().getResourceAsStream("/aas/AAS_Motor_1_Type_Submodel_TechnicalData.json").readAllBytes()))
		);
		when(mockedHttpClient.execute(argThat(httpUriRequest ->
				httpUriRequest != null && httpUriRequest.getURI().toString().contains("/submodels/")))).thenReturn(submodelResponse);

		var client = new AasClient("http://example.org/") {
			@Override
			protected CloseableHttpClient createHttpClient() {
				return mockedHttpClient;
			}
		};

		var memoryStore = new MemoryStore();
		var repo = new SailRepository(memoryStore);
		repo.setFederatedServiceResolver(new BaseFederatedServiceResolver() {
			public FederatedService createService(String url) {
				return new AasFederatedService(client, this::getExecutorService);
			}
		});
		repo.init();

		try {
			try (var conn = repo.getConnection()) {
				var query = conn.prepareTupleQuery("prefix aas: <https://admin-shell.io/aas/3/0/> " +
						"select * {" +
						"service <aas-api:http://aasx-server:5001> {" +
						"<aas-api:endpoint> <aas-api:shells> ?shell ." +
						"    ?shell aas:idShort ?shellId ." +
						"    ?shell aas:submodels [ <aas-api:resolved> ?sm ] ." +
						"    ?sm aas:semanticId/aas:keys/aas:value \"https://admin-shell.io/ZVEI/TechnicalData/Submodel/1/2\" ." +
						"    ?sm (!<:>)+ ?element ." +
						"    { ?element a aas:Property } union { ?element a aas:MultiLanguageProperty }\n" +
						"    ?element aas:idShort \"ProductClassId\" ; " +
						"        aas:valueId/aas:keys/aas:value ?productClassId ." +
						"}" +
						"}");
				try (var result = query.evaluate()) {
					Assert.assertTrue(result.hasNext());
					while (result.hasNext()) {
						var bindings = result.next();
						Assert.assertEquals("0173-1#01-BAB253#016",
								bindings.getValue("productClassId").stringValue());
					}
				}
			}
		} finally {
			repo.shutDown();
		}
	}
}
