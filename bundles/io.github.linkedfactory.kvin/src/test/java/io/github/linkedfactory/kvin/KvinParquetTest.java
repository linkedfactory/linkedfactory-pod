package io.github.linkedfactory.kvin;

import io.github.linkedfactory.kvin.kvinParquet.KvinParquet;
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class KvinParquetTest {

    final int seed = 200;
    Random random = new Random(seed);
    Kvin kvinParquet = new KvinParquet();


    @Test
    public void shouldDoSimplePut() {
        /*File storeDirectory = new File("/tmp/leveldb-test-" + random.nextInt(1000) + "/");
        Kvin store = new KvinLevelDb(storeDirectory);*/
        try {
            Files.deleteIfExists(Path.of("./target/test.data.parquet"));
            Files.deleteIfExists(Path.of("./target/test.mapping.parquet"));

            kvinParquet.put(generateRandomKvinTuples(5000000, 500, 500));

            File dataFile = new File("./target/test.data.parquet");
            File mappingFile = new File("./target/test.mapping.parquet");
            assertEquals(dataFile.exists(), true);
            assertEquals(mappingFile.exists(), true);

            /*Iterator<KvinTuple> data = generateRandomKvinTuples(5000000, 500, 500);
            while(data.hasNext()) {
                store.put(data.next());
            }*/

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet put() method");
        } finally {
            //store.close();
        }
    }

    private NiceIterator<KvinTuple> generateRandomKvinTuples(int sampleSize, int itemPool, int propertyPool) {
        NiceIterator<KvinTuple> iterator = new NiceIterator<>() {
            int tupleCount = 0;
            int propertyCount = 0;

            int itemCounter = 0, propertyCounter = 0;
            int currentPropertyCount = 0;
            boolean isLoopingProperties = false;
            URI currentItem = null;

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
                }

                URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + ++propertyCounter + "/measured-point-1");
                Object value = generateRandomValue();
                long time = System.currentTimeMillis() / 1000;
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

    @Test
    public void shouldDoFetch() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 5000);
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 22 + "/measured-point-1");
            long limit = 0;

            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);

            /*KvinLevelDb store = new KvinLevelDb(new File("/tmp/leveldb-test-329"));
            IExtendedIterator<KvinTuple> tuples = store.fetch(item, property, null, limit);*/

            assertNotNull(tuples);
            int count = 0;
            long startTime = System.currentTimeMillis();
            while (tuples.hasNext()) {
                KvinTuple t = tuples.next();
                System.out.println(t.toString());
                count++;
            }
            long endtime = System.currentTimeMillis() - startTime;
            System.out.println("Record count  : " + count);
            System.out.println("Lookup time: " + endtime + " ms");
            //assertEquals(count, 9);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp fetch() method");
        }
    }
}
