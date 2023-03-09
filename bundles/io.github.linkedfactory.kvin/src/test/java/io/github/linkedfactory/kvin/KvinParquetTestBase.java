package io.github.linkedfactory.kvin;

import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.lang.RandomStringUtils;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class KvinParquetTestBase {
    final long seed = 200L;
    private Random random;

    public KvinParquetTestBase() {
        random = new Random();
        random.setSeed(seed);
    }

    public NiceIterator<KvinTuple> generateRandomKvinTuples(int sampleSize, int itemPool, int propertyPool) {
        NiceIterator<KvinTuple> iterator = new NiceIterator<>() {
            int tupleCount = 0;
            int propertyCount = 0;
            int chunkCounter = 1;
            int samePropCounter = 0;
            int itemCounter = 0, propertyCounter = 0;
            int currentPropertyCount = 0;
            boolean isLoopingProperties = false;
            URI currentItem = null;
            long time = 1678262948L;


            @Override
            public boolean hasNext() {
                return tupleCount < sampleSize;
            }

            @Override
            public KvinTuple next() {
                isLoopingProperties = currentPropertyCount < propertyCount;
                if (!isLoopingProperties) {
                    propertyCount = getRandomInt(500);
                    currentPropertyCount = 0;
                    itemCounter++;
                    propertyCounter = 0;
                    currentItem = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + itemCounter);

                    // incrementing week after n items
                    if (itemCounter % 50 == 0 && itemCounter != 0) {
                        time = time + (604800 * chunkCounter);
                        chunkCounter++;
                    }
                }

                URI property = null;
                // adding multiple property to the same item
                if (itemCounter == 2 && samePropCounter < 10) {
                    property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + propertyCounter + "/measured-point-1");
                    samePropCounter++;
                    if (samePropCounter == 5) {
                        time = time + (604800 * chunkCounter);
                        chunkCounter++;
                    }
                } else {
                    property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + ++propertyCounter + "/measured-point-1");
                }
                Object value = generateRandomValue();
                // adding only int as a value in item with id 2
                if (itemCounter == 2) {
                    value = getRandomInt(500000);
                }
                int seqNr = 0;
                URI context = Kvin.DEFAULT_CONTEXT;
                tupleCount++;
                currentPropertyCount++;
                if (tupleCount % 10000 == 0) {
                    System.out.println("wrote " + tupleCount + " tuples");
                }
                return new KvinTuple(currentItem, property, context, time, seqNr, value);
            }

            @Override
            public void close() {
                super.close();
            }
        };

        return iterator;

    }

    private Object generateRandomValue() {
        String[] dataTypes = {"int", "long", "float", "double", "string", "boolean", "record", "uri"};
        Object value = null;
        switch (dataTypes[ThreadLocalRandom.current().nextInt(dataTypes.length)]) {
            case "int":
                value = getRandomInt(Integer.MAX_VALUE);
                break;
            case "long":
                value = ThreadLocalRandom.current().nextLong(5556028233L, Long.MAX_VALUE);
                break;
            case "float":
                value = ThreadLocalRandom.current().nextFloat() * (500.42f - 10.88f);
                break;
            case "double":
                value = ThreadLocalRandom.current().nextDouble(1.144545455, 500.7976931348623157E30);
                break;
            case "string":
                value = getRandomString(10);
                break;
            case "boolean":
                value = ThreadLocalRandom.current().nextBoolean();
                break;
            case "record":
                value = new Record(URIs.createURI("http://localhost:8080/linkedfactory/demofactory/record"), 55.2565);
                break;
            case "uri":
                value = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/uri");
                break;
        }
        return value;
    }

    private String getRandomString(int stringLength) {
        return RandomStringUtils.random(stringLength, true, false);
    }

    private int getRandomInt(int max) {
        return random.nextInt(max);
    }
}
