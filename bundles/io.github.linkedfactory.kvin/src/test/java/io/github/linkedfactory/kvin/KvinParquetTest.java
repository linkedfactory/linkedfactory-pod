package io.github.linkedfactory.kvin;

import io.github.linkedfactory.kvin.kvinParquet.KvinParquet;
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.hsqldb.lib.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class KvinParquetTest extends KvinParquetTestBase {
    Kvin kvinParquet = new KvinParquet();

    @Test
    public void shouldDoSimplePut() {
        /*File storeDirectory = new File("/tmp/leveldb-test-" + random.nextInt(1000) + "/");
        Kvin store = new KvinLevelDb(storeDirectory);*/
        try {
            // deleting existing files
            FileUtils.deleteDirectory(new File("./target/archive"));
            kvinParquet.put(generateRandomKvinTuples(100000000, 500, 10));

            File mappingFile = new File("./target/archive/data.mapping.parquet");
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

    @Test
    public void shouldDoFetch() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 20000);
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 15 + "/measured-point-1");
            long limit = 0;

            //IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, 1677678374, 1677678274, limit, 100, "avg");
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

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet fetch() method");
        }
    }

    @Test
    public void shouldFetchProperties() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 30000);

            IExtendedIterator<URI> properties = kvinParquet.properties(item);

            assertNotNull(properties);
            int count = 0;
            long startTime = System.currentTimeMillis();
            while (properties.hasNext()) {
                URI p = properties.next();
                //System.out.println(p.toString());
                count++;
            }
            long endtime = System.currentTimeMillis() - startTime;
            System.out.println("Property count  : " + count);
            System.out.println("Lookup time: " + endtime + " ms");
            assertEquals(count, 414);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet properties() method");
        }
    }
}
