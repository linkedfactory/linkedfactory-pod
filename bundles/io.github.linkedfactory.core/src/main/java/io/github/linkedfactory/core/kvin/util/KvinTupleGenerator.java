package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

import java.util.Random;

public class KvinTupleGenerator {
	final static long seed = 200L;
	static String[] ALL_DATA_TYPES = {"int", "long", "float", "double", "string", "boolean", "record", "uri", "array"};
	String[] dataTypes = ALL_DATA_TYPES;
	long startTime;
	int timeDistancePerValue = 10;
	int items;
	int propertiesPerItem;
	int valuesPerProperty;
	String itemPattern;
	String propertyPattern;

	private Random random = new Random(seed);

	public KvinTupleGenerator setDataTypes(String... dataTypes) {
		this.dataTypes = dataTypes;
		return this;
	}

	public KvinTupleGenerator setStartTime(long startTime) {
		this.startTime = startTime;
		return this;
	}

	public KvinTupleGenerator setItems(int items) {
		this.items = items;
		return this;
	}

	public KvinTupleGenerator setPropertiesPerItem(int propertiesPerItem) {
		this.propertiesPerItem = propertiesPerItem;
		return this;
	}

	public KvinTupleGenerator setValuesPerProperty(int valuesPerProperty) {
		this.valuesPerProperty = valuesPerProperty;
		return this;
	}

	public KvinTupleGenerator setItemPattern(String itemPattern) {
		this.itemPattern = itemPattern;
		return this;
	}

	public KvinTupleGenerator setPropertyPattern(String propertyPattern) {
		this.propertyPattern = propertyPattern;
		return this;
	}

	public KvinTupleGenerator setRandom(Random random) {
		this.random = random;
		return this;
	}

	public KvinTupleGenerator setTimeDistancePerValue(int timeDistancePerValue) {
		this.timeDistancePerValue = timeDistancePerValue;
		return this;
	}

	public NiceIterator<KvinTuple> generate() {
		int[] propertyTypes = new int[propertiesPerItem];
		for (int i = 0; i < propertyTypes.length; i++) {
			propertyTypes[i] = random.nextInt(dataTypes.length);
		}
		return new NiceIterator<>() {
			boolean done = false;
			long time = startTime;
			URI item = URIs.createURI(itemPattern.replace("{}", "1"));
			int itemNr = 1;
			URI property = URIs.createURI(propertyPattern.replace("{}", "1"));
			int propertyNr = 1;
			int valueNr = 0;
			KvinTuple next;

			@Override
			public boolean hasNext() {
				if (done) {
					return false;
				}
				if (next == null) {
					if (valueNr < valuesPerProperty) {
						valueNr++;
					} else {
						valueNr = 1;
						if (propertyNr < propertiesPerItem) {
							propertyNr++;
							property = URIs.createURI(propertyPattern.replace("{}",
									String.valueOf(propertyNr)));
						} else {
							propertyNr = 1;
							property = URIs.createURI(propertyPattern.replace("{}",
									String.valueOf(propertyNr)));
							if (itemNr < items) {
								itemNr++;
							} else {
								done = true;
								return false;
							}
							item = URIs.createURI(itemPattern.replace("{}", String.valueOf(itemNr)));
						}
					}
					Object value = generateRandomValue(propertyTypes[propertyNr - 1]);
					next = new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, time, 0, value);
					time += timeDistancePerValue;
				}
				return next != null;
			}

			@Override
			public KvinTuple next() {
				KvinTuple result = next;
				next = null;
				return result;
			}
		};

	}

	private Object generateRandomValue(int typeIndex) {
		Object value = null;
		switch (dataTypes[typeIndex]) {
			case "int":
				value = getRandomInt(Integer.MAX_VALUE);
				break;
			case "long":
				value = random.nextLong();
				break;
			case "float":
				value = random.nextFloat() * 500;
				break;
			case "double":
				value = random.nextDouble() * 500;
				break;
			case "string":
				value = getRandomString(10);
				break;
			case "boolean":
				value = random.nextBoolean();
				break;
			case "record":
				value = new Record(URIs.createURI("property:p1"), 55.2565)
						.append(new Record(URIs.createURI("property:p2"), 25.2565));
				break;
			case "uri":
				value = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/uri");
				break;
			case "array":
				value = new Object[] {2.0, true, "test", new Record(URIs.createURI("property:p1"), 55.2565)};
				break;
		}
		return value;
	}

	private String getRandomString(int stringLength) {
		int leftLimit = 48; // numeral '0'
		int rightLimit = 122; // letter 'z'
		return random.ints(leftLimit, rightLimit + 1)
				.filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(stringLength)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

	private int getRandomInt(int max) {
		return random.nextInt(max);
	}
}
