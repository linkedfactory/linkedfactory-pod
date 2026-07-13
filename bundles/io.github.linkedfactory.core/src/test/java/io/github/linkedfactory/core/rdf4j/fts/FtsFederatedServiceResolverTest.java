package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FtsFederatedServiceResolverTest {
	@Test
	public void usesConfiguredBackendFactoryAndEndpointOverride() throws Exception {
		AtomicReference<String> endpointRef = new AtomicReference<>();
		FtsSearchBackendFactory factory = new FtsSearchBackendFactory() {
			@Override
			public String backendType() {
				return "custom";
			}

			@Override
			public FtsSearchBackend create(FtsFederatedServiceConfig config) {
				endpointRef.set(config.getEndpoint());
				return request -> List.of();
			}
		};
		FtsFederatedServiceResolver resolver = new FtsFederatedServiceResolver(
				new FtsFederatedServiceConfig("custom", "http://default:9200", "/fts/_search", true, 100),
				List.of(factory));

		var service = resolver.getService("fts:http://override:9200");
		assertNotNull(service);
		assertTrue(service instanceof FtsFederatedService);
		assertTrue("http://override:9200".equals(endpointRef.get()));
	}

	@Test
	public void failsForUnknownBackend() throws Exception {
		FtsFederatedServiceResolver resolver = new FtsFederatedServiceResolver(
				new FtsFederatedServiceConfig("unknown", "http://default:9200", "/fts/_search", true, 100),
				List.of(new ElasticKeywordSearchBackendFactory()));
		try {
			resolver.getService("fts:");
			fail("Expected QueryEvaluationException");
		} catch (QueryEvaluationException expected) {
			assertTrue(expected.getMessage().contains("Unknown FTS backend"));
		}
	}
}
