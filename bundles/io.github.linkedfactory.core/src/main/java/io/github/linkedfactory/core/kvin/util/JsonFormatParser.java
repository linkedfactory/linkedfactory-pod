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
import java.util.Iterator;
import java.util.Map;

public class JsonFormatParser {
    final static Logger logger = LoggerFactory.getLogger(JsonFormatParser.class);
    final static JsonFactory jsonFactory = new JsonFactory().configure(Feature.AUTO_CLOSE_SOURCE, true);
    final static ObjectMapper mapper = new ObjectMapper()
            .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    JsonParser parser;

    public JsonFormatParser(InputStream content) throws IOException {
        parser = jsonFactory.createParser(content);
    }

    public NiceIterator<KvinTuple> parse() {
        return parse(System.currentTimeMillis());
    }

    public NiceIterator<KvinTuple> parse(long currentTime) {
        return new NiceIterator<>() {
            KvinTuple kvinTuple;
            URI currentItem;
            URI currentProperty;
            State state = State.PARSE_ITEMS;

            @Override
            public boolean hasNext() {
                if (kvinTuple != null) {
                    return true;
                }
                try {
                    JsonToken token = null;
                    do {
                        switch (state) {
                            case PARSE_ITEMS:
                                while ((token = parser.nextToken()) != null) {
                                    if (token == JsonToken.FIELD_NAME) {
                                        try {
                                            String itemName = parser.currentName();
                                            if (itemName == null || itemName.isEmpty()) {
                                                throw new IOException("Item name is missing or empty in JSON input.");
                                            }
                                            currentItem = URIs.createURI(itemName);
                                        } catch (Exception e) {
                                            throw new IOException("Invalid item URI in JSON input: " + parser.currentName(), e);
                                        }
                                        state = State.PARSE_PROPERTIES;
                                        break;
                                    } else if (token != JsonToken.START_OBJECT && token != JsonToken.END_OBJECT) {
                                        throw new IOException("Expected FIELD_NAME or object delimiters at items level, got: " + token);
                                    }
                                }
                                break;
                            case PARSE_PROPERTIES:
                                while ((token = parser.nextToken()) != null) {
                                    if (token == JsonToken.FIELD_NAME) {
                                        try {
                                            String propertyName = parser.currentName();
                                            if (propertyName == null || propertyName.isEmpty()) {
                                                throw new IOException("Property name is missing or empty in JSON input.");
                                            }
                                            currentProperty = URIs.createURI(propertyName);
                                        } catch (Exception e) {
                                            throw new IOException("Invalid property URI in JSON input: " + parser.currentName(), e);
                                        }
                                        state = State.PARSE_VALUES;
                                        break;
                                    } else if (token == JsonToken.END_OBJECT) {
                                        state = State.PARSE_ITEMS;
                                        break;
                                    } else if (token != JsonToken.START_ARRAY && token != JsonToken.START_OBJECT) {
                                        throw new IOException("Expected FIELD_NAME or END_OBJECT at properties level, got: " + token);
                                    }
                                }
                                break;
                            case PARSE_VALUES:
                                boolean foundValue = false;
                                while ((token = parser.nextToken()) != JsonToken.END_ARRAY && token != null) {
                                    if (token == JsonToken.START_OBJECT) {
                                        JsonNode node = mapper.readTree(parser);
                                        if (node == null || !node.has("value")) {
                                            throw new IOException(String.format("Missing 'value' field for item %s and property %s", currentItem, currentProperty));
                                        }
                                        Object value = nodeToValue(node.get("value"));
                                        Object seqNr = nodeToValue(node.get("seqNr"));
                                        JsonNode timeNode = node.get("time");
                                        Number time = timeNode != null ? (Number) nodeToValue(timeNode) : null;
                                        if (value != null) {
                                            kvinTuple = new KvinTuple(currentItem, currentProperty, Kvin.DEFAULT_CONTEXT,
                                                    time != null ? time.longValue() : currentTime,
                                                    seqNr != null ? ((Number) seqNr).intValue() : 0, value);
                                            foundValue = true;
                                            break;
                                        } else {
                                            throw new IOException(String.format("Invalid null value for item %s and property %s", currentItem, currentProperty));
                                        }
                                    } else if (token != JsonToken.START_ARRAY) {
                                       throw new IOException(String.format("Unexpected token %s in values array for item %s and property %s: %s", token, currentItem, currentProperty, token));
                                    }
                                }
                                if (token == JsonToken.END_ARRAY) {
                                    state = State.PARSE_PROPERTIES;
                                }
                                if (!foundValue && token == null) {
                                    throw new IOException(String.format("Unexpected end of input while parsing values for item %s and property %s", currentItem, currentProperty));
                                }
                                break;
                        }
                    } while (kvinTuple == null && token != null);
                } catch (Exception e) {
                    logger.error("Exception while parsing", e);
                    try {
                        if (parser != null) {
                            parser.close();
                            parser = null;
                        }
                    } catch (IOException ioe) {
                        // ignore
                        logger.error("Exception while closing JSON parser", ioe);
                    }
                    throw new RuntimeException("Error while parsing JSON input: " + e.getMessage(), e);
                }
                return kvinTuple != null;
            }

            @Override
            public KvinTuple next() {
                KvinTuple tuple = kvinTuple;
                kvinTuple = null;
                return tuple;
            }

            @Override
            public void close() {
                try {
                    if (parser != null) {
                        parser.close();
                        parser = null;
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

    enum State {
        PARSE_ITEMS, PARSE_PROPERTIES, PARSE_VALUES
    }
}