package io.github.linkedfactory.core.rdf4j.aas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.linkedfactory.core.kvin.Record;
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
	private static Logger logger = LoggerFactory.getLogger(AasClient.class);

	final String endpoint;
	ObjectMapper mapper = new ObjectMapper();
	CloseableHttpClient httpClient;
	static URI AAS_INDEX_PROPERTY = URIs.createURI(AAS.AAS_NAMESPACE + "index");
	static boolean USE_AAS_INDEX_PROPERTY = true;
	static Set<String> orderedElements = Set.of("keys", "SubmodelElementList", "ValueList");

	public AasClient(String endpoint) {
		this.endpoint = endpoint;
		this.httpClient = createHttpClient();
	}

	protected HttpGet createHttpGet(String endpoint) {
		return new HttpGet(endpoint);
	}

	public IExtendedIterator<Record> shells() throws URISyntaxException, IOException {
		return query(endpoint, "shells", null, null);
	}

	public IExtendedIterator<Record> shell(String id, boolean encodeBase64) throws URISyntaxException, IOException {
		return query(endpoint, "shells/" + (encodeBase64 ?
				Base64.getEncoder().encodeToString(id.getBytes(StandardCharsets.UTF_8)) : id), null, null);
	}

	public IExtendedIterator<Record> submodels() throws URISyntaxException, IOException {
		return query(endpoint, "submodels", null, null);
	}

	public IExtendedIterator<Record> submodel(String id, boolean encodeBase64) throws URISyntaxException, IOException {
		return query(endpoint, "submodels/" + (encodeBase64 ?
				Base64.getEncoder().encodeToString(id.getBytes(StandardCharsets.UTF_8)) : id), null, null);
	}

	protected IExtendedIterator<Record> query(String endpoint, String path, Map<String, String> params, String cursor)
			throws URISyntaxException, IOException {
		URIBuilder uriBuilder = new URIBuilder(endpoint);
		uriBuilder.setPath(Optional.ofNullable(uriBuilder.getPath())
				.map(p -> p.replaceFirst("/?$", "/"))
				.orElse("") + path);
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
				IExtendedIterator<Record> it = WrappedIterator.create(result.iterator()).mapWith(n -> (Record) nodeToValue(n));
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
				return WrappedIterator.create(Collections.singleton((Record) nodeToValue(node)).iterator());
			}
		} finally {
			if (response != null) {
				try {
					response.close();
				} catch (IOException ioe) {
					logger.error("Error while closing response", ioe);
				}
			}
		}
	}

	private Object nodeToValue(JsonNode node) {
		if (node == null) {
			return null;
		}

		if (node.isArray()) {
			List<Object> values = new ArrayList<>();
			for (JsonNode element : node) {
				values.add(nodeToValue(element));
			}
			return values;
		} else if (node.isObject()) {
			Record value = Record.NULL;
			Iterator<Map.Entry<String, JsonNode>> records = node.fields();
			String modelType = null;
			while (records.hasNext()) {
				Map.Entry<String, JsonNode> recordNode = records.next();
				if ("modelType".equals(recordNode.getKey())) {
					modelType = recordNode.getValue().asText();
				}
				URI property = URIs.createURI(AAS.AAS_NAMESPACE + recordNode.getKey());
				JsonNode nodeValue = recordNode.getValue();

				if (nodeValue.isArray() && (USE_AAS_INDEX_PROPERTY || !orderedElements.contains(recordNode.getKey()))) {
					boolean addIndex = USE_AAS_INDEX_PROPERTY && orderedElements.contains(recordNode.getKey());
					// each element of the array is added as (unordered) property value
					int i = 0;
					for (JsonNode element : nodeValue) {
						var converted = nodeToValue(element);
						if (addIndex && converted instanceof Record) {
							converted = ((Record)converted).append(new Record(AAS_INDEX_PROPERTY, BigInteger.valueOf(i++)));
						}
						value = value.append(new Record(property, converted));
					}
				} else {
					// convert value as whole (object or ordered list/array)
					value = value.append(new Record(property, nodeToValue(nodeValue)));
				}
			}
			if (modelType != null) {
				Record newRecord = new Record(RDF.PROPERTY_TYPE, URIs.createURI(AAS.AAS_NAMESPACE + modelType));
				value = value.append(newRecord);
			}
			return value;
		} else if (node.isDouble()) {
			return node.asDouble();
		} else if (node.isFloat()) {
			return Float.parseFloat(node.asText());
		} else if (node.isInt()) {
			return node.asInt();
		} else if (node.isBigInteger()) {
			return new BigInteger(node.asText());
		} else if (node.isBigDecimal()) {
			return new BigDecimal(node.asText());
		} else if (node.isLong()) {
			return node.asLong();
		} else if (node.isShort()) {
			return Short.parseShort(node.asText());
		} else if (node.isBoolean()) {
			return node.asBoolean();
		} else if (node.isTextual()) {
			return node.textValue();
		} else {
			return node;
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
