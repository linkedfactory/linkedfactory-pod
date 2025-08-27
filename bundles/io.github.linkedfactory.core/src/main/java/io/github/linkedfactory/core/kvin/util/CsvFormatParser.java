/*
 * Copyright (c) 2024 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.core.kvin.util;

import com.google.common.math.DoubleMath;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.util.Pair;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CsvFormatParser {
	protected final static Logger logger = LoggerFactory.getLogger(CsvFormatParser.class);
	protected final static Pattern itemProperty = Pattern.compile("(?:((?:\\@|\\>|[^>@])+)@)?((?:\\@|\\>|[^>@])+)");
	protected final URI base;
	protected final List<Pair<URI, URI>> itemProperties;
	protected CSVReader csvReader;
	protected char separator;
	protected URI context = Kvin.DEFAULT_CONTEXT;

	// RFC 3986 path characters
	private static final Pattern URI_REGEX = Pattern.compile(
			"^(?:[a-zA-Z][a-zA-Z0-9+.-]*:[^\\s]+"
					+ "|[a-zA-Z0-9._~!$&'()*+,;=:@/\\-]+)$"
	);

	public CsvFormatParser(URI base, char separator, InputStream content) throws IOException {
		this.base = base;
		this.separator = separator;
		CSVParser parser = new CSVParserBuilder()
				.withSeparator(separator)
				.withIgnoreQuotations(true)
				.build();

		csvReader = new CSVReaderBuilder(new InputStreamReader(content))
				.withSkipLines(0)
				.withCSVParser(parser)
				.build();
		try {
			String[] header = csvReader.readNext();
			if (header != null) {
				itemProperties = parseHeader(header, 1);
			} else {
				itemProperties = null;
			}
		} catch (CsvValidationException e) {
			throw new IOException(e);
		}
	}

	URI toUri(String uriOrName) {
		if (uriOrName == null) {
			uriOrName = "";
		}
		URI uri = null;
		String uriString = null;
		if (uriOrName.startsWith("<") && uriOrName.endsWith(">")) {
			uriString = uriOrName.substring(1, uriOrName.length() - 1);
		} else if (uriOrName.isEmpty()) {
			uri = base;
		} else {
			uriString = uriOrName;
		}
		if (uriString != null) {
			if (!URI_REGEX.matcher(uriString).matches()) {
				throw new InputMismatchException(String.format("Invalid URI: %s.", uriString));
			}
			uri = URIs.createURI(uriString);
			if (uri.isRelative()) {
				uri = base.appendLocalPart(uriString);
			}
		}
		return uri;
	}

	List<Pair<URI, URI>> parseHeader(String[] header, int startIndex) {
		List<Pair<URI, URI>> itemProperties = new ArrayList<>(header.length);
		for (int i = 0; i < startIndex; i++) {
			itemProperties.add(null);
		}
		for (int i = startIndex; i < header.length; i++) {
			Matcher m = itemProperty.matcher(header[i].trim());
			if (m.matches()) {
				URI itemUri = toUri(m.group(1));
				URI propertyUri = toUri(m.group(2));
				itemProperties.add(new Pair<>(itemUri, propertyUri));
			} else {
				itemProperties.add(null);
			}
		}
		return itemProperties;
	}

	public NiceIterator<KvinTuple> parse() {
		return new NiceIterator<KvinTuple>() {
			String[] line;
			KvinTuple tuple;
			long time;
			int column;

			void nextLine() throws CsvValidationException, IOException {
				if (csvReader != null) {
					column = 1;
					line = csvReader.readNext();
					if (line != null) {
						Long timeValue = Longs.tryParse(line[0]);
						if (timeValue != null) {
							time = timeValue;
						} else {
							String index = line[0];
							line = null;
							throw new CsvValidationException(
									String.format("Invalid time format %s; expected long timestamp", index));
						}
					}
				}
			}

			@Override
			public boolean hasNext() {
				if (tuple != null) {
					return true;
				}
				try {
					if (line == null) {
						nextLine();
					}
					if (line != null) {
						while (tuple == null) {
							if (column >= line.length || column >= itemProperties.size()) {
								nextLine();
								if (line == null) {
									break;
								}
							}

							Pair<URI, URI> itemProperty = itemProperties.get(column);
							if (itemProperty != null && column < line.length) {
								String valueStr = line[column].trim();
								Object value = parseValue(valueStr);
								tuple = new KvinTuple(itemProperty.getFirst(), itemProperty.getSecond(),
										context, time, value);
							}
							column++;
						}
					}
				} catch (Exception e) {
					logger.error("Exception while parsing", e);
					try {
						if (csvReader != null) {
							csvReader.close();
							csvReader = null;
						}
					} catch (IOException ioe) {
						// ignore
						logger.error("Exception while closing CSV parser", ioe);
					}
					throw new RuntimeException(e);
				}
				if (tuple == null) {
					close();
				}
				return tuple != null;
			}

			@Override
			public KvinTuple next() {
				ensureHasNext();
				KvinTuple next = tuple;
				tuple = null;
				return next;
			}

			@Override
			public void close() {
				try {
					if (csvReader != null) {
						csvReader.close();
						csvReader = null;
					}
				} catch (IOException e) {
					// ignore
					logger.error("Exception while closing CSV parser", e);
				}
			}
		};
	}

	Object parseValue(String valueStr) {
		if (valueStr.startsWith("\"") && valueStr.endsWith("\"") ||
				valueStr.startsWith("'") && valueStr.endsWith("'")) {
			// this is definitely a string
			return valueStr.substring(1, valueStr.length() - 1);
		}
		// handle boolean values
		String valueStrLowerCase = valueStr.toLowerCase();
		if ("true".equals(valueStrLowerCase)) {
			return true;
		} else if ("false".equals(valueStrLowerCase)) {
			return false;
		}
		Object value = valueStr;
		Double doubleValue = Doubles.tryParse(valueStr);
		if (doubleValue == null && valueStr.contains(",")) {
			String cleanedValueStr;
			if (valueStr.lastIndexOf(',') < valueStr.lastIndexOf('.')) {
				// convert numbers like 123,456.78 to 123456.78
				cleanedValueStr = valueStr.replaceAll(",", "");
			} else {
				// convert numbers like 123.456,78 to 123456.78
				cleanedValueStr = valueStr.replaceAll("[.]", "")
						.replaceAll(",", ".");
			}
			doubleValue = Doubles.tryParse(cleanedValueStr);
		}
		if (doubleValue != null) {
			if (DoubleMath.isMathematicalInteger(doubleValue)) {
				value = doubleValue.longValue();
			} else {
				value = doubleValue;
			}
		}
		// TODO - support json values
		return value;
	}

	public CsvFormatParser setContext(URI context) {
		this.context = context;
		return this;
	}

	public URI getContext() {
		return context;
	}
}
