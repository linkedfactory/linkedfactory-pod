package io.github.linkedfactory.service.rdf4j.aas;

import com.fasterxml.jackson.core.JsonFactory;
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

public class AasClient implements Closeable {
	ObjectMapper mapper = new ObjectMapper();
	CloseableHttpClient httpClient;
	JsonFactory jsonFactory = new JsonFactory();

	public AasClient() {
		this.httpClient = createHttpClient();
	}

	protected HttpGet createHttpGet(String endpoint) {
		return new HttpGet(endpoint);
	}

	public IExtendedIterator<Object> shells(String endpoint) throws URISyntaxException, IOException {
		URIBuilder uriBuilder = new URIBuilder(endpoint);
		uriBuilder.setPath("shells");
		java.net.URI getRequestUri = uriBuilder.build();

		// sending get request to the endpoint
		HttpGet httpGet = createHttpGet(getRequestUri.toString());
		HttpResponse response = this.httpClient.execute(httpGet);
		HttpEntity entity = response.getEntity();
		if (response.getStatusLine().getStatusCode() != 200) {
			return NiceIterator.emptyIterator();
		}
		JsonNode node = mapper.readTree(entity.getContent());
		JsonNode result = node.get("result");
		if (result != null && result.isArray()) {
			return WrappedIterator.create(result.iterator()).mapWith(n -> nodeToValue(n));
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
