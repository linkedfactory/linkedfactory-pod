package io.github.linkedfactory.kvin;

import io.github.linkedfactory.kvin.kvinParquet.KvinParquet;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class KvinParquetTest {

    @Test
    public void shouldDoSimplePut() {
        try {
            Files.deleteIfExists(Path.of("./target/test.parquet"));
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/machine1/sensor1");
            URI property1 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/value");
            URI property2 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/flag");
            URI property3 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/block");

            Kvin kvinParquet = new KvinParquet();
            KvinTuple tuple1 = new KvinTuple(item, property1, Kvin.DEFAULT_CONTEXT, 1676023233957L, 0, 2);
            KvinTuple tuple2 = new KvinTuple(item, property1, Kvin.DEFAULT_CONTEXT, 1676023233957L, 0, 5556028233957L);
            KvinTuple tuple3 = new KvinTuple(item, property2, Kvin.DEFAULT_CONTEXT, 1676023191326L, 0, 2.14f);
            KvinTuple tuple4 = new KvinTuple(item, property2, Kvin.DEFAULT_CONTEXT, 1676023191326L, 0, 2.144545455);
            KvinTuple tuple5 = new KvinTuple(item, property2, Kvin.DEFAULT_CONTEXT, 1676023191326L, 0, "str");
            KvinTuple tuple6 = new KvinTuple(item, property2, Kvin.DEFAULT_CONTEXT, 1676023191326L, 0, true);
            KvinTuple tuple7 = new KvinTuple(item, property3, Kvin.DEFAULT_CONTEXT, 1676023191326L, 0, new Record(URIs.createURI("http://example.com/vocab/record"), 55.2565));
            KvinTuple tuple8 = new KvinTuple(item, property3, Kvin.DEFAULT_CONTEXT, 1676023191326L, 0, URIs.createURI("http://example.com/vocab/uri"));
            kvinParquet.put(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8);

            File f = new File("./target/test.parquet");
            assertEquals(f.exists(), true);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet put() method");
        }
    }

    @Test
    public void shouldDoFetch() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/machine1/sensor1");
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/value");
            long limit = 0;

            Kvin kvinParquet = new KvinParquet();
            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, null, limit);
            assertNotNull(tuples);
            int count = 0;
            while (tuples.hasNext()) {
                KvinTuple t = tuples.next();
                switch (count) {
                    case 0:
                        assertTrue(t.value instanceof Integer);
                        break;
                    case 1:
                        assertTrue(t.value instanceof Long);
                        break;
                    case 2:
                        assertTrue(t.value instanceof Float);
                        break;
                    case 3:
                        assertTrue(t.value instanceof Double);
                        break;
                    case 4:
                        assertTrue(t.value instanceof String);
                        break;
                    case 5:
                        assertTrue(t.value instanceof Boolean);
                        break;
                    case 6:
                        assertTrue(t.value instanceof Record);
                        break;
                    case 7:
                        assertTrue(t.value instanceof URI);
                        break;
                }
                count++;
            }
            assertEquals(count, 8);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp fetch() method");
        }
    }
}
