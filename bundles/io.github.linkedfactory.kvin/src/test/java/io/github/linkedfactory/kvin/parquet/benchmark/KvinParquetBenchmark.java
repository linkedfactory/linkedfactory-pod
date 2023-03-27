package io.github.linkedfactory.kvin.parquet.benchmark;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.parquet.KvinParquetTestBase;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.parquet.KvinParquet;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;


@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class KvinParquetBenchmark extends KvinParquetTestBase {
    Kvin kvinParquet = new KvinParquet();
    @Param({"10000", "12000", "14000", "22000", "28000", "36000", "42000", "50000"})
    private int itemIds;

    // run this method to add benchmark data
    public void benchmarkWriteOperation() {

        try {
            // deleting existing files
            FileUtils.deleteDirectory(new File("./target/archive"));
            kvinParquet.put(generateRandomKvinTuples(100000000, 500, 10));

        } catch (Exception e) {
            fail("Something went wrong while testing KvinParquet put() method");
        }
    }

    @Benchmark
    public void benchmarkSingleTupleRead() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + itemIds);
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/febric/" + 5 + "/measured-point-1");
            long limit = 0;
            long startTime = System.currentTimeMillis();

            //IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, 1677678374, 1677678274, limit, 100, "avg");
            IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);

            Assert.assertNotNull(tuples);
            int count = 0;
            while (tuples.hasNext()) {
                KvinTuple t = tuples.next();
                //System.out.println(t.toString());
                count++;
            }
            long endtime = System.currentTimeMillis() - startTime;
            //System.out.println("Record count  : " + count);
            //System.out.println("Lookup time: " + endtime + " ms");

        } catch (Exception e) {
            Assert.fail("Something went wrong while testing KvinParquet fetch() method");
        }
    }

    @Benchmark
    public void benchmarkPropertiesRead() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + itemIds);

            IExtendedIterator<URI> properties = kvinParquet.properties(item);

            Assert.assertNotNull(properties);
            int count = 0;
            long startTime = System.currentTimeMillis();
            while (properties.hasNext()) {
                URI p = properties.next();
                //System.out.println(p.toString());
                count++;
            }
            long endtime = System.currentTimeMillis() - startTime;
            //System.out.println("Property count  : " + count);
            //System.out.println("Lookup time: " + endtime + " ms");
            //assertEquals(count, 414);

        } catch (Exception e) {
            Assert.fail("Something went wrong while testing KvinParquet properties() method");
        }
    }
}
