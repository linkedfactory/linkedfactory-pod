package io.github.linkedfactory.kvin.parquet;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class KvinParquetTest extends KvinParquetTestBase {
    static KvinParquet kvinParquet;
    static File tempDir;

    @BeforeClass
    public static void setup() throws IOException {
        tempDir = Files.createTempDirectory("archive").toFile();
        kvinParquet = new KvinParquet(tempDir.getAbsolutePath() + "/");

        try {
            kvinParquet.put(generateRandomKvinTuples(5000, 500, 10));
            assertTrue(new File(tempDir.getPath()).listFiles().length > 0);
            nonSequentialPut();


        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet put() method");
        }
    }

    @AfterClass
    public static void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(tempDir.getPath()));
    }

    public static void nonSequentialPut() {
        int propCount = 1;
        URI item1 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 9000);
        URI item2 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 9001);
        URI item4 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 9002);
        URI item5 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 9003);

        // inserting as a new week
        ArrayList<KvinTuple> tuples = new ArrayList<>();
        tuples.add(new KvinTuple(item1, URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + propCount + "/measured-point-1"), Kvin.DEFAULT_CONTEXT, 1697022611, 0, 11.00));
        propCount++;
        tuples.add(new KvinTuple(item1, URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + propCount + "/measured-point-1"), Kvin.DEFAULT_CONTEXT, 1697022612, 0, 12.00));
        propCount++;
        tuples.add(new KvinTuple(item2, URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + propCount + "/measured-point-1"), Kvin.DEFAULT_CONTEXT, 1697022613, 0, 13.00));
        propCount++;
        kvinParquet.put(tuples);

        // inserting as existing week
        tuples.clear();
        tuples.add(new KvinTuple(item4, URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + propCount + "/measured-point-1"), Kvin.DEFAULT_CONTEXT, 1697022615, 0, 13.00));
        propCount++;
        tuples.add(new KvinTuple(item5, URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + propCount + "/measured-point-1"), Kvin.DEFAULT_CONTEXT, 1697022616, 0, 14.00));
        kvinParquet.put(tuples);
    }


    private File getNonSeqInsertFolder() {
        File nonSeqInsertFolder = null;

        File[] archiveFolders = tempDir.listFiles();
        for (File folder : archiveFolders) {
            if (folder.getName().startsWith("2023")) {
                File[] subFolders = folder.listFiles();
                for (File subFolder : subFolders) {
                    if (subFolder.getName().startsWith("41")) {
                        nonSeqInsertFolder = subFolder;
                        break;
                    }
                }
            }
            if (nonSeqInsertFolder != null) break;
        }
        return nonSeqInsertFolder;
    }

    @Test
    public void testNonSeqPut() {
        File nonSeqFolder = getNonSeqInsertFolder();
        assertTrue(nonSeqFolder.getName().startsWith("41_"));
        assertEquals(3, nonSeqFolder.listFiles().length);
    }

    @Test
    public void shouldDoFetch() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 10);
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 1 + "/measured-point-1");
            long limit = 0;

            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);
            assertNotNull(tuples);
            assertTrue(tuples.toList().size() > 0);
            tuples.close();
        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet fetch() method");
        }
    }

    @Test
    public void shouldFetchAllProperties() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
            long limit = 0;

            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, null, Kvin.DEFAULT_CONTEXT, limit);
            assertNotNull(tuples);
            assertEquals(29, tuples.toList().size());
            tuples.close();
        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet shouldDoFetchWithLimit() method");
        }
    }

    @Test
    public void shouldDoFetchWithLimit() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 2);
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 0 + "/measured-point-1");
            long limit = 5;

            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);
            assertNotNull(tuples);
            assertEquals(5, tuples.toList().size());
            tuples.close();
        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet shouldDoFetchWithLimit() method");
        }
    }

    @Test
    public void shouldDoFetchForNonSeqEntry() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 9000);
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 1 + "/measured-point-1");
            long limit = 0;

            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);
            assertNotNull(tuples);
            assertTrue(tuples.toList().size() > 0);
            tuples.close();
        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet fetch() method");
        }
    }

    @Test
    public void mappingFileCompactionTest() throws InterruptedException {
        kvinParquet.startCompactionWorker(0, 5, TimeUnit.SECONDS);
        Thread.sleep(1000);
        File[] metadataFiles = new File(kvinParquet.archiveLocation + "metadata").listFiles((file, s) -> s.endsWith(".parquet"));
        assertEquals(3, metadataFiles.length);

        File nonSeqFolder = getNonSeqInsertFolder();
        File[] dataFiles = nonSeqFolder.listFiles((file, s) -> s.endsWith(".parquet"));
        assertEquals(1, dataFiles.length);

        kvinParquet.stopCompactionWorker();
    }

    @Test
    public void shouldFetchProperties() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
            IExtendedIterator<URI> properties = kvinParquet.properties(item);
            assertNotNull(properties);
            assertEquals(29, properties.toList().size());
            properties.close();
        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet properties() method");
        }
    }
}
