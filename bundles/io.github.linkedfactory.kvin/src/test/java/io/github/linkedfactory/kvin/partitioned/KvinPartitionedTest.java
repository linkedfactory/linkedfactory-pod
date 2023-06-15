package io.github.linkedfactory.kvin.partitioned;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@Ignore
public class KvinPartitionedTest extends KvinPartitionedTestBase {
    static KvinPartitioned kvinPartitioned;
    static File tempDir;
    File archiveDir = new File("./target/archive");

    @BeforeClass
    public static void setup() {
        try {
            tempDir = Files.createTempDirectory("temp_levelDb").toFile();
            kvinPartitioned = new KvinPartitioned(tempDir, 2, TimeUnit.SECONDS); // archive at every 2 seconds

        } catch (Exception e) {
            fail("Something went wrong while running setup method of KvinPartitioned test.");
        }
    }

    @AfterClass
    public static void cleanup() {
        try {
            kvinPartitioned.close();
            FileUtils.deleteDirectory(new File(tempDir.getAbsolutePath()));
            FileUtils.deleteDirectory(new File("./target/archive"));
        } catch (Exception e) {
            fail("Something went wrong while running cleanup method of KvinPartitioned test.");
        }
    }

    @Test
    public void shouldDoPut() {
        try {
            kvinPartitioned.put(generateTestTuples(10, 0, 1672614000L));
            Thread.sleep(500);
            // trying to read while archival is in process
            TestArchivalInProcessRead testArchivalInProcessRead = new TestArchivalInProcessRead();
            Thread inProcessArchivalReadTestThread = new Thread(testArchivalInProcessRead);
            inProcessArchivalReadTestThread.start();
            inProcessArchivalReadTestThread.join();
            boolean isInArchivalReadSuccessful = testArchivalInProcessRead.getIsInArchivalReadSuccessful();
            assertTrue(isInArchivalReadSuccessful);

            // continuing incremental put on kvinPartitioned
            Thread.sleep(1000);
            kvinPartitioned.put(generateTestTuples(10, 5, 1673218800L));
            Thread.sleep(2000);
            kvinPartitioned.archivalTaskfuture.cancel(false);
            kvinPartitioned.put(generateTestTuples(10, 10, 1673823600L));

            NiceIterator<KvinTuple> storeIterator = kvinPartitioned.readCurrentHotStore();
            assertTrue(archiveDir.listFiles().length > 0); // main folder
            assertEquals(2, archiveDir.listFiles()[0].listFiles().length); // sub folders
            int recordCount = 0;
            while (storeIterator.hasNext()) {
                storeIterator.next();
                recordCount++;
            }
            assertEquals(recordCount, 10);

        } catch (Exception e) {
            fail("Something went wrong while running shouldDoPut method of KvinPartitioned test.");
        }
    }

    @Test
    public void shouldDoFetch() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 9);
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 1 + "/measured-point-1");
            long limit = 0;

            IExtendedIterator<KvinTuple> tuples = kvinPartitioned.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);

            assertNotNull(tuples);
            assertTrue(tuples.toList().size() > 0);

        } catch (Exception e) {
            fail("Something went wrong while testing shouldDoFetch method of KvinPartitioned test.");
        }
    }

    @Test
    public void shouldFetchProperties() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 5);
            IExtendedIterator<URI> properties = kvinPartitioned.properties(item);

            assertNotNull(properties);
            assertTrue(properties.toList().size() > 0);
        } catch (Exception e) {
            fail("Something went wrong while testing properties method of KvinPartitioned test.");
        }
    }

    class TestArchivalInProcessRead implements Runnable {
        AtomicBoolean isInArchivalReadSuccessful = new AtomicBoolean(false);

        @Override
        public void run() {
            while (true) {
                if (kvinPartitioned.isArchivalInProcess()) {
                    URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 4);
                    URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 1 + "/measured-point-1");
                    long limit = 0;

                    IExtendedIterator<KvinTuple> tuples = kvinPartitioned.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);

                    assertNotNull(tuples);
                    assertTrue(tuples.toList().size() > 0);
                    isInArchivalReadSuccessful.set(true);
                    break;
                }
            }
        }

        public boolean getIsInArchivalReadSuccessful() {
            return isInArchivalReadSuccessful.get();
        }
    }
}
