package io.github.linkedfactory.core.rdf4j.aas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import net.enilink.vocab.rdf.RDF;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;

public class AasClient implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(AasClient.class);

	final String endpoint;
	ObjectMapper mapper = new ObjectMapper();
	CloseableHttpClient httpClient;
	static URI AAS_INDEX_PROPERTY = URIs.createURI(AAS.AAS_NAMESPACE + "index");
	static boolean USE_AAS_INDEX_PROPERTY = true;
	static Set<String> orderedElements = Set.of("keys", "SubmodelElementList", "ValueList");
	final Model cache = new LinkedHashModel();
	final ValueFactory valueFactory;

	public AasClient(String endpoint, ValueFactory valueFactory) {
		this.endpoint = endpoint;
		this.valueFactory = valueFactory;
		this.httpClient = createHttpClient();
	}

	protected HttpGet createHttpGet(String endpoint) {
		return new HttpGet(endpoint);
	}

	public IExtendedIterator<IRI> shells() throws URISyntaxException, IOException {
		return query(endpoint, "shells", null, null);
	}

	public IExtendedIterator<IRI> shell(String id, boolean encodeBase64) throws URISyntaxException, IOException {
		return query(endpoint, "shells/" + (encodeBase64 ?
				Base64.getEncoder().encodeToString(id.getBytes(StandardCharsets.UTF_8)) : id), null, null);
	}

	public IExtendedIterator<IRI> submodels() throws URISyntaxException, IOException {
		return query(endpoint, "submodels", null, null);
	}

	public IExtendedIterator<IRI> submodel(String id, boolean encodeBase64) throws URISyntaxException, IOException {
		return query(endpoint, "submodels/" + (encodeBase64 ?
				Base64.getEncoder().encodeToString(id.getBytes(StandardCharsets.UTF_8)) : id), null, null);
	}

	public Model getCache() {
		return cache;
	}

	protected IExtendedIterator<IRI> query(String endpoint, String path, Map<String, String> params, String cursor)
			throws URISyntaxException, IOException {
		URIBuilder uriBuilder = new URIBuilder(endpoint);
		uriBuilder.setPath(Optional.ofNullable(uriBuilder.getPath())
				.map(p -> p.replaceFirst("/?$", "/"))
				.orElse("") + path);
		IRI base = valueFactory.createIRI(uriBuilder.toString());
		if (params != null) {
			params.forEach((k, v) -> uriBuilder.setParameter(k, v));
		}
		if (cursor != null) {
			uriBuilder.setParameter("cursor", cursor);
		}
		java.net.URI getRequestUri = uriBuilder.build();
		System.out.println(getRequestUri);

		// sending get request to the endpoint
		HttpGet httpGet = createHttpGet(getRequestUri.toString());
		var response = this.httpClient.execute(httpGet);
		HttpEntity entity = response.getEntity();
		try (InputStream content = entity.getContent()) {
			if (response.getStatusLine().getStatusCode() != 200) {
				return NiceIterator.emptyIterator();
			}
			JsonNode node = mapper.readTree(content);
			JsonNode result = node.get("result");
			if (result != null && result.isArray()) {
				JsonNode pagingMetaData = node.get("paging_metadata");
				JsonNode cursorNode = null;
				if (pagingMetaData != null) {
					cursorNode = pagingMetaData.get("cursor");
				}
				IExtendedIterator<IRI> it = WrappedIterator.create(result.iterator())
						.mapWith(n -> nodeToIRI(n, base, "", ""));
				if (cursorNode != null) {
					String nextCursor = cursorNode.asText();
					// use lazy iterator here to ensure that request is only executed when required
					it = it.andThen(new LazyIterator<>(() -> {
						try {
							return query(endpoint, path, params, nextCursor);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}));
				}
				return it;
			} else {
				return WrappedIterator.create(Collections.singleton(nodeToIRI(node, base, "", "")).iterator());
			}
		} finally {
			try {
				response.close();
			} catch (IOException ioe) {
				logger.error("Error while closing response", ioe);
			}
		}
	}

	private IRI nodeToIRI(JsonNode node, IRI rootUri, String shortIdPath, String genIdPath) {
		Iterator<Map.Entry<String, JsonNode>> records = node.fields();
		String id = node.has("id") ? node.get("id").asText() : null;
		IRI subject;
		if (id != null) {
			rootUri = valueFactory.createIRI(AAS.stringToUri(id));
			subject = valueFactory.createIRI(rootUri.toString());
		} else {
			String idShort = node.has("idShort") ? node.get("idShort").asText() : null;
			if (idShort != null) {
				shortIdPath = shortIdPath.isEmpty() ? idShort : shortIdPath + "." + idShort;
				genIdPath = shortIdPath;
			}
			subject = valueFactory.createIRI(rootUri.stringValue() + "#" + genIdPath);
		}

		String modelType = null;
		while (records.hasNext()) {
			Map.Entry<String, JsonNode> recordNode = records.next();
			if ("modelType".equals(recordNode.getKey())) {
				modelType = recordNode.getValue().asText();
			}

			URI property = URIs.createURI(AAS.AAS_NAMESPACE + recordNode.getKey());
			IRI predicate = valueFactory.createIRI(property.toString());

			JsonNode nodeValue = recordNode.getValue();
			if (nodeValue.isArray() && (USE_AAS_INDEX_PROPERTY || !orderedElements.contains(recordNode.getKey()))) {
				boolean addIndex = USE_AAS_INDEX_PROPERTY && orderedElements.contains(recordNode.getKey());
				int i = 0;
				for (JsonNode element : nodeValue) {
					Value object = nodeToRdfValue(element, rootUri, shortIdPath,
							genIdPath + (genIdPath.isEmpty() ? "" : ".") + recordNode.getKey() + "." + i);
					cache.add(subject, predicate, object, rootUri);
					if (addIndex) {
						IRI indexPredicate = valueFactory.createIRI(AAS_INDEX_PROPERTY.toString());
						cache.add(subject, indexPredicate, valueFactory.createLiteral(i), rootUri);
					}
					i++;
				}
			} else {
				Value object = nodeToRdfValue(nodeValue, rootUri, shortIdPath,
						genIdPath + (genIdPath.isEmpty() ? "" : ".") + recordNode.getKey());
				cache.add(subject, predicate, object, rootUri);
			}
		}
		if (modelType != null) {
			IRI typePredicate = valueFactory.createIRI(RDF.PROPERTY_TYPE.toString());
			IRI typeObject = valueFactory.createIRI(AAS.AAS_NAMESPACE + modelType);
			cache.add(subject, typePredicate, typeObject);
		}
		return subject;
	}

	private Value nodeToRdfValue(JsonNode node, IRI rootUri, String shortIdPath, String genIdPath) {
		if (node == null) {
			return valueFactory.createLiteral("");
		}
		if (node.isObject()) {
			return nodeToIRI(node, rootUri, shortIdPath, genIdPath);
		} else if (node.isDouble()) {
			return valueFactory.createLiteral(node.asDouble());
		} else if (node.isFloat()) {
			return valueFactory.createLiteral(Float.parseFloat(node.asText()));
		} else if (node.isInt()) {
			return valueFactory.createLiteral(node.asInt());
		} else if (node.isBigInteger()) {
			return valueFactory.createLiteral(new BigInteger(node.asText()));
		} else if (node.isBigDecimal()) {
			return valueFactory.createLiteral(new BigDecimal(node.asText()));
		} else if (node.isLong()) {
			return valueFactory.createLiteral(node.asLong());
		} else if (node.isShort()) {
			return valueFactory.createLiteral(Short.parseShort(node.asText()));
		} else if (node.isBoolean()) {
			return valueFactory.createLiteral(node.asBoolean());
		} else if (node.isTextual()) {
			return valueFactory.createLiteral(node.textValue());
		} else {
			return valueFactory.createLiteral(node.toString());
		}
	}

	protected CloseableHttpClient createHttpClient() {
		return HttpClients.createDefault();
	}

	@Override
	public void close() throws IOException {
		if (this.httpClient != null) {
			this.httpClient.close();
			this.httpClient = null;
		}
	}

	static class LazyIterator<T> extends NiceIterator<T> {
		Supplier<IExtendedIterator<T>> factory;
		IExtendedIterator<T> it;

		public LazyIterator(Supplier<IExtendedIterator<T>> factory) {
			this.factory = factory;
		}

		@Override
		public boolean hasNext() {
			if (it == null && factory != null) {
				it = factory.get();
			}
			return it.hasNext();
		}

		@Override
		public T next() {
			ensureHasNext();
			return it.next();
		}

		@Override
		public void close() {
			if (it != null) {
				it.close();
				it = null;
			}
			factory = null;
		}
	}
}
