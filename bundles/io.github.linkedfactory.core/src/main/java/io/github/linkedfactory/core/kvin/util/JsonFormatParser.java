package io.github.linkedfactory.core.kvin.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class JsonFormatParser {
    final static Logger logger = LoggerFactory.getLogger(JsonFormatParser.class);
    final static JsonFactory jsonFactory = new JsonFactory().configure(Feature.AUTO_CLOSE_SOURCE, true);
    final static ObjectMapper mapper = new ObjectMapper()
               .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    JsonParser jsonParser;

    public JsonFormatParser(InputStream content) throws IOException {
        jsonParser = jsonFactory.createParser(content);
    }

    public NiceIterator<KvinTuple> parse() {
        return parse(System.currentTimeMillis());
    }

    public NiceIterator<KvinTuple> parse(long currentTime) {
        return new NiceIterator<>() {
            KvinTuple kvinTuple;
            URI currentItem;
            URI currentProperty;
            boolean isLoopingProperties = false;
            boolean isLoopingPropertyItems = false;

            @Override
            public boolean hasNext() {
                try {
                    if (kvinTuple == null && isLoopingPropertyItems) {
                        boolean isAddingPropertyItems = addNextPropertyItem(currentItem, currentProperty);
                        if (!isAddingPropertyItems) {
                            boolean isAddingProperty = addNextProperty();
                            if (!isAddingProperty) {
                                addNextItem();
                            }
                        }
                        return isTuplesAlreadyGenerated();

                    } else if (kvinTuple == null && isLoopingProperties) {
                        boolean isAddingProperty = addNextProperty();
                        if (!isAddingProperty) {
                            addNextItem();
                        }
                        return isTuplesAlreadyGenerated();

                    } else if (kvinTuple == null) {
                        addNextItem();
                        return isTuplesAlreadyGenerated();

                    } else {
                        return isTuplesAlreadyGenerated();
                    }
                } catch (Exception e) {
                    logger.error("Exception while parsing", e);
                    try {
                        if (jsonParser != null) {
                            jsonParser.close();
                            jsonParser = null;
                        }
                    } catch (IOException ioe) {
                        // ignore
                        logger.error("Exception while closing JSON parser", ioe);
                    }
                    throw new RuntimeException(e);
                }
            }

            @Override
            public KvinTuple next() {
                KvinTuple tuple = kvinTuple;
                kvinTuple = null;
                return tuple;
            }

            private void addNextItem() throws IOException {
                while (jsonParser.nextToken() != null) {
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME) {
                        currentItem = URIs.createURI(jsonParser.getCurrentName());
                        addNextProperty();
                        if (isTuplesAlreadyGenerated()) {
                            break;
                        }
                    }
                }
            }

            private boolean addNextProperty() throws IOException {
                while (jsonParser.nextToken() != null) {
                    isLoopingProperties = true;
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME) {
                        currentProperty = URIs.createURI(jsonParser.getCurrentName());
                        jsonParser.nextToken();

                        addNextPropertyItem(currentItem, currentProperty);
                        jsonParser.skipChildren();
                    }
                    if (isTuplesAlreadyGenerated()) break;
                }
                if (kvinTuple == null && jsonParser.getCurrentToken() == JsonToken.END_OBJECT) {
                    isLoopingProperties = false;
                }
                return isTuplesAlreadyGenerated();
            }

            private boolean addNextPropertyItem(URI item, URI property) throws IOException {
                JsonToken token;
                while ((token = jsonParser.nextToken()) != JsonToken.END_ARRAY && token != null) {
                    isLoopingPropertyItems = true;
                    if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                        JsonNode node = mapper.readTree(jsonParser);
                        Object value = nodeToValue(node.get("value"));
                        Object seqNr = nodeToValue(node.get("seqNr"));
                        Number time = (Number) nodeToValue(node.get("time"));
                        if (value != null) {
                            kvinTuple = new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT,
                                    time != null ? time.longValue() : currentTime,
                                    seqNr != null ? ((Number) seqNr).intValue() : 0, value);
                            break;
                        } else {
                            throw new IOException(String.format("Invalid null value for item %s and property %s", item, property));
                        }
                    }
                }
                if (kvinTuple == null && jsonParser.getCurrentToken() == JsonToken.END_ARRAY) {
                    isLoopingPropertyItems = false;
                }
                return isTuplesAlreadyGenerated();
            }

            private boolean isTuplesAlreadyGenerated() {
                return kvinTuple != null;
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
    }

    private Object nodeToValue(JsonNode node) {
        if (node == null) {
            return null;
        }

        Record value;
        if (node.isObject()) {
            JsonNode idNode = node.get("@id");
            if (idNode != null) {
                return URIs.createURI(node.get("@id").textValue());
            }

            Iterator<Map.Entry<String, JsonNode>> records = node.fields();
            value = Record.NULL;
            while (records.hasNext()) {
                Map.Entry<String, JsonNode> recordNode = records.next();
                value = value.append(new Record(URIs.createURI(recordNode.getKey()), nodeToValue(recordNode.getValue())));
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
}