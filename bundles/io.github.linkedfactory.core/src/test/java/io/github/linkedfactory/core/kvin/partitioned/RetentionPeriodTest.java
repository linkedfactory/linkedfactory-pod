package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.Assert.*;

public class RetentionPeriodTest {
    static String itemTemplate = "http://localhost:8080/linkedfactory/demofactory/{}";
    static String propertyTemplate = "http://example.org/{}";
    KvinTupleGenerator tupleGenerator;
    KvinPartitioned kvinPartitioned;
    File tempDir;

    @Before
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("kvinPartitioned").toFile();
        tupleGenerator = new KvinTupleGenerator().setItems(10).setPropertiesPerItem(10).setValuesPerProperty(10).setItemPattern(itemTemplate).setPropertyPattern(propertyTemplate);
        kvinPartitioned = new KvinPartitioned(tempDir, null, Duration.ZERO);
    }

    @After
    public void cleanup() throws IOException {
        kvinPartitioned.close();
        FileUtils.deleteDirectory(tempDir);
    }

    @Test
    public void testMultiThreading() throws InterruptedException, IOException {
        Path archivePath = tempDir.toPath().resolve("archive");
        Path currentPath = tempDir.toPath().resolve("current");
        kvinPartitioned.put(tupleGenerator.setStartTime(1672614000000L).generate());

        URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
        URI item2 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 2);
        URI item3 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 3);
        URI property = URIs.createURI("http://example.org/" + 1);
        IExtendedIterator<KvinTuple> it1 = kvinPartitioned.fetch(item, property, Kvin.DEFAULT_CONTEXT, 2);
        it1.hasNext();
        // assure the archive folder not yet created
        assertFalse(archivePath.toFile().exists());
        assertTrue(currentPath.toFile().exists());
        Thread archiver = new Thread(() -> {
            System.out.println("start archival");
            kvinPartitioned.runArchival();
            // assure the archive folder created
            assertTrue(archivePath.toFile().exists());
            System.out.println("finished archival");
        });
        archiver.start();

        for (KvinTuple t1 : it1) {
            System.out.println("t1: " + t1);
            IExtendedIterator<KvinTuple> it2 = kvinPartitioned.fetch(item2, property, Kvin.DEFAULT_CONTEXT, 1);
            while (it2.hasNext()) {
                System.out.println("t2: " + it2.next());
            }
        }

        Thread.sleep(10);

        for (KvinTuple t3 : kvinPartitioned.fetch(item3, property, Kvin.DEFAULT_CONTEXT, 2)) {
            System.out.println("t3: " + t3);
        }

        archiver.join();
        // assure that the data was archived
        boolean archived = Files.walk(archivePath, 3).skip(1).anyMatch(p -> p.toFile().getName().startsWith("data"));
        assertTrue(archived);
        kvinPartitioned.runArchival();
        // assure that old data before the current moment removed
        archived = Files.walk(archivePath, 3).skip(1).anyMatch(p -> p.toFile().getName().startsWith("data"));
        assertFalse(archived);
    }

}
