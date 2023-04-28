package io.github.linkedfactory.kvin.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URIs;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class JsonFormatParser {
    final static JsonFactory jsonFactory = new JsonFactory().configure(Feature.AUTO_CLOSE_SOURCE, true);
    final JsonParser jsonParser;
    final static ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);

    public JsonFormatParser(InputStream content) throws IOException {
        jsonParser = jsonFactory.createParser(content);
    }

    public NiceIterator<KvinTuple> parse() {
        return new NiceIterator<>() {
            KvinTuple kvinTuple;
            String currentItemName;
            String currentPropertyName;
            boolean isLoopingProperties = false;
            boolean isLoopingPropertyItems = false;

            @Override
            public boolean hasNext() {
                try {
                    if (kvinTuple == null && isLoopingPropertyItems) {
                        boolean isAddingPropertyItems = addNextPropertyItem(currentItemName, currentPropertyName);
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
                } catch (IOException e) {
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
                        currentItemName = jsonParser.getCurrentName();
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
                        currentPropertyName = jsonParser.getCurrentName();
                        jsonParser.nextToken();

                        addNextPropertyItem(currentItemName, currentPropertyName);
                        jsonParser.skipChildren();
                    }
                    if (isTuplesAlreadyGenerated()) break;
                }
                if (kvinTuple == null && jsonParser.getCurrentToken() == JsonToken.END_OBJECT) {
                    isLoopingProperties = false;
                }
                return isTuplesAlreadyGenerated();
            }

            private boolean addNextPropertyItem(String itemName, String propertyName) throws IOException {
                while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                    isLoopingPropertyItems = true;
                    if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                        JsonNode node = mapper.readTree(jsonParser);
                        Object value = nodeToValue(node.get("value"));
                        Object seqNr = nodeToValue(node.get("seqNr"));
                        kvinTuple = new KvinTuple(URIs.createURI(itemName), URIs.createURI(propertyName), null,
                            ((Number) nodeToValue(node.get("time"))).longValue(), seqNr != null ?  ((Number) seqNr).intValue() : 0, value);
                        break;
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
                    jsonParser.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        };
    }

    private Object nodeToValue(JsonNode node) {
        if (node == null) {
            return null;
        }

        Record value = null;
        if (node.isObject() && node.get("@id") != null) {
            return URIs.createURI(node.get("@id").textValue());
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


}
