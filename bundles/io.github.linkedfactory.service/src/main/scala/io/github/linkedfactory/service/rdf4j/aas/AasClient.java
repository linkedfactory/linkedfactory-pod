package io.github.linkedfactory.service.rdf4j.aas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.linkedfactory.kvin.Record;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URIs;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class AasClient implements Closeable {
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

	ObjectMapper mapper = new ObjectMapper();
	CloseableHttpClient httpClient;

	public AasClient() {
		this.httpClient = createHttpClient();
	}

	protected HttpGet createHttpGet(String endpoint) {
		return new HttpGet(endpoint);
	}

	public IExtendedIterator<Record> shells(String endpoint) throws URISyntaxException, IOException {
		return query(endpoint, "shells", null);
	}

	public IExtendedIterator<Record> submodels(String endpoint) throws URISyntaxException, IOException {
		return query(endpoint, "submodels", null);
	}

	protected IExtendedIterator<Record> query(String endpoint, String collection, String cursor) throws URISyntaxException, IOException {
		URIBuilder uriBuilder = new URIBuilder(endpoint);
		uriBuilder.setPath(collection);
		if (cursor != null) {
			uriBuilder.setParameter("cursor", cursor);
		}
		java.net.URI getRequestUri = uriBuilder.build();

		// sending get request to the endpoint
		HttpGet httpGet = createHttpGet(getRequestUri.toString());
		HttpResponse response = this.httpClient.execute(httpGet);
		HttpEntity entity = response.getEntity();
		if (response.getStatusLine().getStatusCode() != 200) {
			return NiceIterator.emptyIterator();
		}
		JsonNode node = mapper.readTree(entity.getContent());
		JsonNode pagingMetaData = node.get("paging_metadata");
		JsonNode cursorNode = null;
		if (pagingMetaData != null) {
			cursorNode = pagingMetaData.get("cursor");
		}
		JsonNode result = node.get("result");
		if (result != null && result.isArray()) {
			IExtendedIterator<Record> it = WrappedIterator.create(result.iterator()).mapWith(n -> (Record) nodeToValue(n));
			if (cursorNode != null) {
				String nextCursor = cursorNode.asText();
				// use lazy iterator here to ensure that request is only executed when required
				it = it.andThen(new LazyIterator<>(() -> {
					try {
						return query(endpoint, collection, nextCursor);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}));
			}
			return it;
		}
		return NiceIterator.emptyIterator();
	}

	private Object nodeToValue(JsonNode node) {
		if (node == null) {
			return null;
		}

		Record value = null;
		if (node.isArray()) {
			List<Object> values = new ArrayList<>();
			for (JsonNode element : node) {
				values.add(nodeToValue(element));
			}
			return values;
		} else if (node.isObject()) {
			Iterator<Map.Entry<String, JsonNode>> records = node.fields();
			while (records.hasNext()) {
				Map.Entry<String, JsonNode> recordNode = records.next();
				if (value != null) {
					value = value.append(new Record(URIs.createURI(recordNode.getKey()), nodeToValue(recordNode.getValue())));
				} else {
					value = new Record(URIs.createURI(recordNode.getKey()), nodeToValue(recordNode.getValue()));
				}
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
}
