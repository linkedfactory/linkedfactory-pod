package io.github.linkedfactory.kvin;

import io.github.linkedfactory.kvin.kvinParquet.KvinParquet;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class KvinParquetTest extends KvinParquetTestBase {
    Kvin kvinParquet = new KvinParquet();
    @Test
    public void shouldDoSimplePut() {
        try {
            // deleting existing files
            FileUtils.deleteDirectory(new File("./target/archive"));
            kvinParquet.put(generateRandomKvinTuples(50, 500, 10));

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet put() method");
        }
    }

    @Test
    public void shouldDoFetch() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 1 + "/measured-point-1");
            long limit = 0;

            //IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, 1677678374, 1677678274, limit, 100, "avg");
            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);

            assertNotNull(tuples);
            int count = 0;
            while (tuples.hasNext()) {
                KvinTuple t = tuples.next();
                count++;
            }
            assertEquals(count > 0, true);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet fetch() method");
        }
    }

    @Test
    public void shouldFetchProperties() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);

            IExtendedIterator<URI> properties = kvinParquet.properties(item);

            assertNotNull(properties);
            int count = 0;
            while (properties.hasNext()) {
                URI p = properties.next();
                count++;
            }
            assertEquals(count > 0, true);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet properties() method");
        }
    }
}
