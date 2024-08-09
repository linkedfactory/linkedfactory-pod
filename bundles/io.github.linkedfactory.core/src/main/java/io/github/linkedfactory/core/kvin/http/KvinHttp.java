package io.github.linkedfactory.core.kvin.http;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.ByteStreams;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.util.JsonFormatParser;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class KvinHttp implements Kvin {
    private static Logger logger = LoggerFactory.getLogger(KvinHttp.class);
    private static int MAX_GET_URL_LENGTH = 6000; // the absolute maximum should be 8192 (8kb)
    String hostEndpoint;
    ArrayList<KvinListener> listeners = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    CloseableHttpClient httpClient;
    JsonFactory jsonFactory = new JsonFactory();

    public KvinHttp(String hostEndpoint) {
        this.hostEndpoint = hostEndpoint.endsWith("/") ? hostEndpoint.substring(0, hostEndpoint.length() - 1) : hostEndpoint;
        this.httpClient = getHttpClient();
    }

    public CloseableHttpClient getHttpClient() {
        return HttpClients.createDefault();
    }

    @Override
    public boolean addListener(KvinListener listener) {
        try {
            listeners.add(listener);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean removeListener(KvinListener listener) {
        try {
            listeners.remove(listener);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(KvinTuple... tuples) {
        try {
            this.put(Arrays.asList(tuples));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Iterable<KvinTuple> tuples) {
        try {
            // grouping
            Map<URI, Map<URI, List<KvinTuple>>> groupedData = new HashMap<>();
            for (KvinTuple tuple : tuples) {
                Map<URI, List<KvinTuple>> propertyData = groupedData.computeIfAbsent(tuple.item, (item) -> new HashMap<>());
                List<KvinTuple> values = propertyData.computeIfAbsent(tuple.property, (property) -> new ArrayList<>());
                values.add(tuple);
            }
            // converting tuples to json
            ObjectNode rootNode = mapper.createObjectNode();
            for (Map.Entry<URI, Map<URI, List<KvinTuple>>> data : groupedData.entrySet()) {
                ObjectNode predicateNode = mapper.createObjectNode();
                for (Map.Entry<URI, List<KvinTuple>> property : data.getValue().entrySet()) {
                    ArrayList<ObjectNode> objectList = new ArrayList<>();
                    for (KvinTuple tuple : property.getValue()) {
                        ObjectNode objectNode = mapper.createObjectNode();
                        objectNode.set("value", objectToJson(tuple.value));
                        objectNode.put("time", tuple.time);
                        objectNode.put("seqNr", tuple.seqNr);
                        objectList.add(objectNode);
                    }
                    predicateNode.set(property.getKey().toString(), mapper.createArrayNode().addAll(objectList));
                }
                rootNode.set(data.getKey().toString(), predicateNode);
            }
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
            // sending post request to the remote endpoint
            HttpPost httpPost = createHttpPost(this.hostEndpoint + "/values");
            StringEntity requestPayload = new StringEntity(
                    json,
                    ContentType.APPLICATION_JSON
            );
            httpPost.setEntity(requestPayload);
            CloseableHttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // The method will return the passed object converted to jackson JsonNode
    private JsonNode objectToJson(Object object) {
        JsonNode rootNode;
        if (object instanceof Record) {
            ObjectNode node = mapper.createObjectNode();
            node.set(((Record) object).getProperty().toString(), mapper.valueToTree(((Record) object).getValue()));
            rootNode = node;
        } else {
            rootNode = mapper.valueToTree(object);
        }
        //handling id -> @id conversion
        if (!rootNode.path("id").isMissingNode() && !rootNode.isTextual()) {
            ObjectNode node = (ObjectNode) rootNode;
            node.set("@id", rootNode.get("id"));
            node.remove("id");
        }
        return rootNode;
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
        return fetchInternal(List.of(item), property == null ? Collections.emptyList() : List.of(property),
                context, null, null, limit, null, null);
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
        return fetchInternal(List.of(item), property == null ? Collections.emptyList() : List.of(property),
                context, end != KvinTuple.TIME_MAX_VALUE ? end : null,
                begin != 0 ? begin : null, limit != 0 ? limit : null, interval != 0 ? interval : null, op);
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(List<URI> items, List<URI> properties, URI context, long end, long begin, long limit, long interval, String op) {
        return fetchInternal(items, properties,
                context, end != KvinTuple.TIME_MAX_VALUE ? end : null,
                begin != 0 ? begin : null, limit != 0 ? limit : null, interval != 0 ? interval : null, op);
    }

    protected IExtendedIterator<KvinTuple> fetchInternal(List<URI> items, List<URI> properties, URI context, Long end, Long begin, Long limit, Long interval, String op) {
        CloseableHttpResponse response = null;
        InputStream content = null;
        try {
            final List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("item",
                    String.join(" ", items.stream().map(URI::toString).collect(Collectors.toSet()))));
            if (!properties.isEmpty()) params.add(new BasicNameValuePair("property",
                    String.join(" ", properties.stream().map(URI::toString).collect(Collectors.toSet()))));
            if (context != null) params.add(new BasicNameValuePair("model", context.toString()));
            if (limit != null) params.add(new BasicNameValuePair("limit", Long.toString(limit)));
            if (end != null) params.add(new BasicNameValuePair("to", Long.toString(end)));
            if (begin != null) params.add(new BasicNameValuePair("from", Long.toString(begin)));
            if (interval != null) params.add(new BasicNameValuePair("interval", Long.toString(interval)));
            if (op != null) params.add(new BasicNameValuePair("op", op));
            String endpointUrl = this.hostEndpoint + "/values";
            // start with length of URL plus '?', '&' and '=' characters
            int approximateSize = endpointUrl.length() + 1 + params.size() * 2;
            for (var pair : params) {
                approximateSize += pair.getName().length() + pair.getValue().length();
            }
            // add some overhead for encoding
            approximateSize = (int) (approximateSize * 1.3);
            HttpUriRequest request;
            // decide if GET or POST should be used to send query
            if (approximateSize > MAX_GET_URL_LENGTH) {
                HttpPost post = createHttpPost(endpointUrl);
                post.setEntity(new UrlEncodedFormEntity(params));
                request = post;
            } else {
                URIBuilder uriBuilder = new URIBuilder(endpointUrl);
                uriBuilder.addParameters(params);
                java.net.URI getRequestUri = uriBuilder.build();
                request = createHttpGet(getRequestUri.toString());
            }
            response = this.httpClient.execute(request);
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != 200) {
                return NiceIterator.emptyIterator();
            }
            // converting json to kvin tuples
            // TODO directly read from stream with pooled HTTP client
            content = entity.getContent();
            JsonFormatParser jsonParser = new JsonFormatParser(new ByteArrayInputStream(ByteStreams.toByteArray(content)));
            return jsonParser.parse();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (content != null) {
                try {
                    content.close();
                } catch (IOException ioe) {
                    logger.error("Error while closing input stream", ioe);
                }
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ioe) {
                    logger.error("Error while closing response", ioe);
                }
            }
        }
    }

    @Override
    public long delete(URI item, URI property, URI context, long end, long begin) {
        return 0;
    }

    @Override
    public boolean delete(URI item, URI context) {
        return false;
    }

    @Override
    public IExtendedIterator<URI> descendants(URI item, URI context) {
        return descendantsInternal(item, context, null);
    }

    @Override
    public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
        return descendantsInternal(item, context, limit);
    }

    private IExtendedIterator<URI> descendantsInternal(URI item, URI context, Long limit) {
        try {
            // building url
            URIBuilder uriBuilder = new URIBuilder(this.hostEndpoint + "/**");
            uriBuilder.setParameter("item", item.toString());
            if (context != null) uriBuilder.setParameter("model", context.toString());
            if (limit != null) uriBuilder.setParameter("limit", Long.toString(limit));
            java.net.URI getRequestUri = uriBuilder.build();
            // sending get request to the endpoint
            HttpGet httpGet = createHttpGet(getRequestUri.toString());
            HttpResponse response = this.httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != 200) {
                return NiceIterator.emptyIterator();
            }
            // converting json to URI
            return new NiceIterator<>() {
                JsonParser jsonParser = jsonFactory.createParser(new ByteArrayInputStream(ByteStreams.toByteArray(entity.getContent())));

                @Override
                public boolean hasNext() {
                    try {
                        return isLoopingArray();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public URI next() {
                    URI descendant = null;
                    try {
                        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                            if (jsonParser.getCurrentToken() == JsonToken.VALUE_STRING) {
                                descendant = URIs.createURI(jsonParser.getValueAsString());
                                break;
                            }
                        }
                        jsonParser.nextToken();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return descendant;
                }

                private boolean isLoopingArray() throws IOException {
                    return jsonParser.nextToken() != JsonToken.END_ARRAY;
                }

                @Override
                public void close() {
                    try {
                        if (jsonParser != null) {
                            jsonParser.close();
                            jsonParser = null;
                        }
                    } catch (IOException e) {
                        // ignore
                        logger.error("Exception while closing JSON parser", e);
                    }
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IExtendedIterator<URI> properties(URI item, URI context) {
        try {
            // building url
            URIBuilder uriBuilder = new URIBuilder(this.hostEndpoint + "/properties");
            uriBuilder.setParameter("item", item.toString());
            if (context != null) uriBuilder.setParameter("model", context.toString());
            java.net.URI getRequestUri = uriBuilder.build();
            // sending get request to the endpoint
            HttpGet httpGet = createHttpGet(getRequestUri.toString());
            HttpResponse response = this.httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != 200) {
                return NiceIterator.emptyIterator();
            }
            // converting json to URI
            return new NiceIterator<>() {
                JsonParser jsonParser = jsonFactory.createParser(new ByteArrayInputStream(ByteStreams.toByteArray(entity.getContent())));

                @Override
                public boolean hasNext() {
                    try {
                        return isLoopingArray();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public URI next() {
                    URI property = null;
                    try {
                        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                            if (jsonParser.getCurrentToken() == JsonToken.VALUE_STRING) {
                                property = URIs.createURI(jsonParser.getValueAsString());
                                break;
                            }
                        }
                        jsonParser.nextToken();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return property;
                }

                private boolean isLoopingArray() throws IOException {
                    return jsonParser.nextToken() != JsonToken.END_ARRAY;
                }

                @Override
                public void close() {
                    try {
                        if (jsonParser != null) {
                            jsonParser.close();
                            jsonParser = null;
                        }
                    } catch (IOException e) {
                        // ignore
                        logger.error("Exception while closing JSON parser", e);
                    }
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.httpClient.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HttpPost createHttpPost(String endpoint) {
        return new HttpPost(endpoint);
    }

    public HttpGet createHttpGet(String endpoint) {
        return new HttpGet(endpoint);
    }
}