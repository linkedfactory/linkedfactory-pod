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
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class JsonFormatParser {
	final static Logger logger = LoggerFactory.getLogger(JsonFormatParser.class);
	final static URI DEFAULT_BASE = URIs.createURI("urn:base:");

	final static JsonFactory jsonFactory = new JsonFactory().configure(Feature.AUTO_CLOSE_SOURCE, true);
	final static ObjectMapper mapper = new ObjectMapper().configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	final JsonParser parser;
	URI context = Kvin.DEFAULT_CONTEXT;
	final URI base;

	public JsonFormatParser(InputStream content) throws IOException {
		this(content, DEFAULT_BASE);
	}

	public JsonFormatParser(InputStream content, URI base) throws IOException {
		this.parser = jsonFactory.createParser(content);
		this.base = base;
	}

	public JsonFormatParser setContext(URI context) {
		this.context = context;
		return this;
	}

	public IExtendedIterator<KvinTuple> parse() {
		return parse(System.currentTimeMillis());
	}

	public IExtendedIterator<KvinTuple> parse(long currentTime) {
		return parseInternal(currentTime, State.PARSE_ITEMS);
	}

	public IExtendedIterator<KvinTuple> parseValues() {
		return parseValues(System.currentTimeMillis());
	}

	public IExtendedIterator<KvinTuple> parseValues(long currentTime) {
		return parseInternal(currentTime, State.PARSE_VALUES);
	}

	protected IExtendedIterator<KvinTuple> parseInternal(long currentTime, State initialState) {
		return new NiceIterator<>() {
			KvinTuple kvinTuple;
			URI currentItem;
			URI currentProperty;
			State state = initialState;

			final Deque<Map<String, String>> activeContexts = new ArrayDeque<>();

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
										String itemName = parser.currentName();
										if ("@context".equals(itemName)) {
											JsonToken contextToken = parser.nextToken();
											if (contextToken != JsonToken.START_OBJECT) {
												throw new IOException("Expected object value for @context, got: " + contextToken);
											}
											JsonNode contextNode = mapper.readTree(parser);
											if (contextNode != null && contextNode.isObject()) {
												activeContexts.addFirst(parseContext(contextNode));
											}
											continue;
										}
										try {
											if (itemName == null || itemName.isEmpty()) {
												throw new IOException("Item name is missing or empty in JSON input.");
											}
											currentItem = resolveUri(itemName, activeContexts, true);
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
											currentProperty = resolveUri(propertyName, activeContexts, false);
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
										kvinTuple = parseValueTuple(currentItem, currentProperty, currentTime, activeContexts);
										foundValue = true;
										break;
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
						parser.close();
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
					parser.close();
				} catch (IOException e) {
					// ignore
					logger.error("Exception while closing JSON parser", e);
				}
			}
		};
	}

	protected KvinTuple parseValueTuple(URI currentItem, URI currentProperty, long currentTime, Deque<Map<String, String>> activeContexts) throws IOException {
		Object value = null;
		Object seqNr = null;
		Number time = null;
		boolean foundValue = false;

		JsonToken token;
		while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
			if (token != JsonToken.FIELD_NAME) {
				throw new IOException(String.format("Expected FIELD_NAME or END_OBJECT in values object for item %s and property %s, got: %s", currentItem, currentProperty, token));
			}

			String fieldName = parser.currentName();
			JsonToken valueToken = parser.nextToken();
			if (valueToken == null) {
				throw new IOException(String.format("Unexpected end of input while parsing value object for item %s and property %s", currentItem, currentProperty));
			}

			if ("value".equals(fieldName)) {
				value = parseJsonValue(valueToken, activeContexts);
				foundValue = true;
			} else if ("seqNr".equals(fieldName)) {
				seqNr = parseJsonValue(valueToken, activeContexts);
			} else if ("time".equals(fieldName)) {
				Object parsedTime = parseJsonValue(valueToken, activeContexts);
				time = parsedTime != null ? (Number) parsedTime : null;
			} else {
				parseJsonValue(valueToken, activeContexts);
			}
		}

		if (!foundValue) {
			throw new IOException(String.format("Missing 'value' field for item %s and property %s", currentItem, currentProperty));
		}
		if (value == null) {
			throw new IOException(String.format("Invalid null value for item %s and property %s", currentItem, currentProperty));
		}

		return new KvinTuple(currentItem, currentProperty, context, time != null ? time.longValue() : currentTime, seqNr != null ? ((Number) seqNr).intValue() : 0, value);
	}

	protected Object parseJsonValue(JsonToken token, Deque<Map<String, String>> activeContexts) throws IOException {
		return switch (token) {
			case START_OBJECT -> parseObjectValue(activeContexts);
			case START_ARRAY -> parseArrayValue(activeContexts);
			case VALUE_STRING -> parser.getText();
			case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> parseNumberValue();
			case VALUE_TRUE -> true;
			case VALUE_FALSE -> false;
			case VALUE_NULL -> null;
			case VALUE_EMBEDDED_OBJECT -> parser.getEmbeddedObject();
			default -> throw new IOException("Unexpected token while parsing JSON value: " + token);
		};
	}

	protected Object parseObjectValue(Deque<Map<String, String>> activeContexts) throws IOException {
		Record value = Record.NULL;
		String id = null;

		JsonToken token;
		while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
			if (token != JsonToken.FIELD_NAME) {
				throw new IOException("Expected FIELD_NAME or END_OBJECT while parsing JSON object, got: " + token);
			}

			String fieldName = parser.currentName();
			JsonToken valueToken = parser.nextToken();
			if (valueToken == null) {
				throw new IOException("Unexpected end of input while parsing JSON object");
			}

			if ("@id".equals(fieldName)) {
				Object idValue = parseJsonValue(valueToken, activeContexts);
				id = idValue != null ? idValue.toString() : null;
			} else if (id == null) {
				value = value.append(new Record(resolveUri(fieldName, activeContexts, false), parseJsonValue(valueToken, activeContexts)));
			} else {
				parseJsonValue(valueToken, activeContexts);
			}
		}

		return id != null ? resolveUri(id, activeContexts, false) : value;
	}

	protected Object parseArrayValue(Deque<Map<String, String>> activeContexts) throws IOException {
		List<Object> values = new ArrayList<>();
		JsonToken token;
		while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
			if (token == null) {
				throw new IOException("Unexpected end of input while parsing JSON array");
			}
			values.add(parseJsonValue(token, activeContexts));
		}
		return values.toArray(new Object[0]);
	}

	protected Number parseNumberValue() throws IOException {
		return switch (parser.getNumberType()) {
			case INT -> parser.getIntValue();
			case LONG -> parser.getLongValue();
			case BIG_INTEGER -> parser.getBigIntegerValue();
			case BIG_DECIMAL -> parser.getDecimalValue();
			case FLOAT -> parser.getFloatValue();
			case DOUBLE -> parser.getDoubleValue();
		};
	}

	protected Map<String, String> parseContext(JsonNode contextNode) {
		Map<String, String> context = new HashMap<>();
		for (Map.Entry<String, JsonNode> contextEntry : contextNode.properties()) {
			JsonNode valueNode = contextEntry.getValue();
			if (valueNode.isTextual()) {
				context.put(contextEntry.getKey(), valueNode.textValue());
			}
		}
		return context;
	}

	protected URI resolveUri(String uriString, Deque<Map<String, String>> contexts, boolean makeAbsolute) {
		int colonIndex = uriString.indexOf(':');
		if (colonIndex > 0 && uriString.substring(colonIndex + 1).startsWith("//")) {
			return createURI(uriString, makeAbsolute ? base : null);
		}

		String prefix = colonIndex >= 0 ? uriString.substring(0, colonIndex) : uriString;
		for (Map<String, String> context : contexts) {
			String prefixValue = context.get(prefix);
			if (prefixValue != null) {
				String suffix = colonIndex >= 0 ? uriString.substring(colonIndex + 1) : uriString.substring(prefix.length());
				String expandedPrefix = resolveUri(prefixValue, contexts, makeAbsolute).toString();
				return createURI(expandedPrefix.concat(suffix), makeAbsolute ? base : null);
			}
		}

		return createURI(uriString, makeAbsolute ? base : null);
	}

	protected URI createURI(String uriString, URI base) {
		if (uriString == null || uriString.isEmpty()) {
			throw new IllegalArgumentException("URI string is null or empty");
		}
		if (containsWhitespace(uriString)) {
			throw new IllegalArgumentException("URI string contains whitespace: '" + uriString + "'");
		}
		// Further URI validation can be added here if needed
		var result = URIs.createURI(uriString);
		if (result.isRelative() && base != null) {
			if (base.isHierarchical()) {
				result = result.resolve(base);
			} else {
				result = base.appendLocalPart(uriString);
			}
		}
		return result;
	}

	protected static boolean containsWhitespace(String str) {
		if (str == null || str.isEmpty()) {
			return false;
		}

		int length = str.length();
		for (int i = 0; i < length; i++) {
			if (Character.isWhitespace(str.charAt(i))) {
				return true; // Short-circuits the moment whitespace is found
			}
		}
		return false;
	}

	protected enum State {
		PARSE_ITEMS, PARSE_PROPERTIES, PARSE_VALUES
	}
}