package io.github.linkedfactory.kvin.kvinparquet;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class KvinParquetTest extends KvinParquetTestBase {
    static Kvin kvinParquet;
    static File tempDir;

    @BeforeClass
    public static void setup() throws IOException {
        tempDir = Files.createTempDirectory("archive").toFile();
        kvinParquet = new KvinParquet(tempDir.getAbsolutePath() + "/");
    }

    @AfterClass
    public static void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(tempDir.getPath()));
    }

    @Test
    public void shouldDoSimplePut() {
        try {
            kvinParquet.put(generateRandomKvinTuples(50, 500, 10));
            assertTrue(new File(tempDir.getPath()).listFiles().length > 0);

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

            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);

            assertNotNull(tuples);
            assertTrue(tuples.toList().size() > 0);

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
            assertTrue(properties.toList().size() > 0);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet properties() method");
        }
    }
}
