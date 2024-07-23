package io.github.linkedfactory.core.kvin.parquet.benchmark;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.parquet.KvinParquet;
import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import io.github.linkedfactory.core.rdf4j.kvin.KvinFederatedService;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1044"})
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class KvinParquetBenchmark {
    static File processUseCaseLevelDbStoreDir, processUseCaseParquetStoreDir, machineUseCaseLevelDbStoreDir, machineUseCaseParquetStoreDir;
    static KvinLevelDb processUseCaseLevelDbStore, machineUseCaseLevelDbStore;
    static KvinParquet processUseCaseParquetStore, machineUseCaseParquetStore;

    String processUseCaseQueryString = "prefix aq: <http://dm.adaproq.de/vocab/>\n" +
            "\n" +
            "select * {\n" +
            "\n" +
            "   service <kvin:> {\n" +
            "     { select distinct ?wp { aq:gtc aq:workpiece [ <kvin:value> ?wp ] . } }\n" +
            "     ?wp aq:abschnitt [ <kvin:value> \"anfang\" ; <kvin:limit> 1 ] .\n" +
            "     ?wp ?property [ <kvin:value> ?v ; <kvin:time> ?t ].\n" +
            "   }  \n" +
            "}";

    String machineUseCaseQueryString = "prefix esw: <http://dm.adaproq.de/datamodel/ESW-M-Schraube_ABC#ESW-M-Presskraefte_1.>\n" +
            "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "\n" +
            "select * {\n" +
            "   # query data for specific parts\n" +
            "   values ?part { <part:6> <part:7> <part:8> <part:9> <part:10> <part:11> }\n" +
            "\n" +
            "   service <kvin:> {\n" +
            "     # angle and force are joined by time and index\n" +
            "    { ?part esw:Channel1.Angle [ <kvin:value> ?a1 ; <kvin:time> ?t1 ; <kvin:index> ?s ] ; esw:Channel1.Force [ <kvin:value> ?f1 ; <kvin:time> ?t1 ; <kvin:index> ?s ] . }\n" +
            "    { ?part esw:Channel2.Angle [ <kvin:value> ?a2 ; <kvin:time> ?t2 ; <kvin:index> ?s ] ; esw:Channel2.Force [ <kvin:value> ?f2 ; <kvin:time> ?t2 ; <kvin:index> ?s ] . }\n" +
            "    { ?part esw:Channel3.Angle [ <kvin:value> ?a3 ; <kvin:time> ?t3 ; <kvin:index> ?s ] ; esw:Channel3.Force [ <kvin:value> ?f3 ; <kvin:time> ?t3 ; <kvin:index> ?s ] . }\n" +
            "    { ?part esw:Channel4.Angle [ <kvin:value> ?a4 ; <kvin:time> ?t4 ; <kvin:index> ?s ] ; esw:Channel4.Force [ <kvin:value> ?f4 ; <kvin:time> ?t4 ; <kvin:index> ?s ] . }\n" +
            "    { ?part esw:Channel5.Angle [ <kvin:value> ?a5 ; <kvin:time> ?t5 ; <kvin:index> ?s ] ; esw:Channel5.Force [ <kvin:value> ?f5 ; <kvin:time> ?t5 ; <kvin:index> ?s ] . }\n" +
            "    { ?part esw:Channel6.Angle [ <kvin:value> ?a6 ; <kvin:time> ?t6 ; <kvin:index> ?s ] ; esw:Channel6.Force [ <kvin:value> ?f6 ; <kvin:time> ?t6 ; <kvin:index> ?s ] . }\n" +
            "   }\n" +
            "} order by ?part ?s";

    public KvinParquetBenchmark() {
        try {
            processUseCaseLevelDbStoreDir = Files.createTempDirectory("temp_levelDb_process").toFile();
            machineUseCaseLevelDbStoreDir = Files.createTempDirectory("temp_levelDb_machine").toFile();

            processUseCaseParquetStoreDir = Files.createTempDirectory("temp_parquet_process").toFile();
            machineUseCaseParquetStoreDir = Files.createTempDirectory("temp_parquet_machine").toFile();

            processUseCaseLevelDbStore = new KvinLevelDb(processUseCaseLevelDbStoreDir);
            machineUseCaseLevelDbStore = new KvinLevelDb(machineUseCaseLevelDbStoreDir);

            processUseCaseParquetStore = new KvinParquet(processUseCaseParquetStoreDir.getAbsolutePath() + "/");
            machineUseCaseParquetStore = new KvinParquet(machineUseCaseParquetStoreDir.getAbsolutePath() + "/");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(KvinParquetBenchmark.class.getSimpleName() + ".testKvinParquetReadPerformanceForProcessUseCase") // adapt to control which benchmark tests to run
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void testKvinLevelDbReadPerformanceForProcessUseCase(KvinParquetBenchmarkBase benchmarkBase, Blackhole blackhole) {
        blackhole.consume(KvinParquetBenchmarkBase.LevelDBProcessUseCaseDataIterator);
        TupleQueryResult result = readFromStore(processUseCaseLevelDbStore, processUseCaseQueryString, "http://dm.adaproq.de/vocab/");
        blackhole.consume(result);
    }

    @Benchmark
    public void testKvinLevelDbReadPerformanceForMachineUseCase(KvinParquetBenchmarkBase benchmarkBase, Blackhole blackhole) {
        blackhole.consume(KvinParquetBenchmarkBase.LevelDBMachineUseCaseDataIterator);
        TupleQueryResult result = readFromStore(machineUseCaseLevelDbStore, machineUseCaseQueryString, "http://dm.adaproq.de/datamodel/ESW-M-Schraube_ABC#ESW-M-Presskraefte_1.");
        blackhole.consume(result);
    }

    @Benchmark
    public void testKvinParquetReadPerformanceForProcessUseCase(KvinParquetBenchmarkBase benchmarkBase, Blackhole blackhole) {
        blackhole.consume(KvinParquetBenchmarkBase.parquetProcessUseCaseDataIterator);
        TupleQueryResult result = readFromStore(processUseCaseParquetStore, processUseCaseQueryString, "http://dm.adaproq.de/vocab/");
        blackhole.consume(result);
    }

    @Benchmark
    public void testKvinParquetReadPerformanceForMachineUseCase(KvinParquetBenchmarkBase benchmarkBase, Blackhole blackhole) {
        blackhole.consume(KvinParquetBenchmarkBase.parquetMachineUseCaseDataIterator);
        TupleQueryResult result = readFromStore(machineUseCaseParquetStore, machineUseCaseQueryString, "http://dm.adaproq.de/datamodel/ESW-M-Schraube_ABC#ESW-M-Presskraefte_1.");
        blackhole.consume(result);
    }

    @Benchmark
    public void testSingleItemReadPerformanceForProcessUseCaseParquetStore(KvinParquetBenchmarkBase benchmarkBase, Blackhole blackhole) {
        URI item = URIs.createURI("http://dm.adaproq.de/vocab/wp1995");
        IExtendedIterator<KvinTuple> tuples = processUseCaseParquetStore.fetch(item, null, Kvin.DEFAULT_CONTEXT, 0);
        while (tuples.hasNext()) {
            KvinTuple tuple = tuples.next();
            blackhole.consume(tuple);
        }
    }

    private TupleQueryResult readFromStore(Kvin store, String query, String baseURI) {
        MemoryStore memoryStore = new MemoryStore();
        SailRepository repository = new SailRepository(memoryStore);
        var resolver = new BaseFederatedServiceResolver() {
            @Override
            protected FederatedService createService(String s) throws QueryEvaluationException {
                return new KvinFederatedService(store, this::getExecutorService, null, false);
            }
        };
        repository.setFederatedServiceResolver(resolver);

        repository.init();
        SailRepositoryConnection conn = repository.getConnection();

        SailTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query, baseURI);
        TupleQueryResult result = tupleQuery.evaluate();
        while (result.hasNext()) {
            result.next();
        }
        result.close();
        conn.close();
        repository.shutDown();
        resolver.shutDown();

        return result;
    }

    @State(Scope.Benchmark)
    public static class KvinParquetBenchmarkBase {
        public static NiceIterator<KvinTuple> LevelDBProcessUseCaseDataIterator, parquetProcessUseCaseDataIterator, LevelDBMachineUseCaseDataIterator, parquetMachineUseCaseDataIterator;

        @Setup(Level.Trial)
        public void setup() {
            ingestProcessUseCaseDataForLevelDbStore();
            ingestProcessUseCaseDataForParquetStore();
            ingestMachineUseCaseDataForLevelDbStore();
            ingestMachineUseCaseDataForParquetStore();
        }

        private void ingestProcessUseCaseDataForLevelDbStore() {
            LevelDBProcessUseCaseDataIterator = generateProcessUseCaseData("http://dm.adaproq.de/vocab/", 2000); // 102000 tuples
            while (LevelDBProcessUseCaseDataIterator.hasNext()) {
                KvinTuple tuple = LevelDBProcessUseCaseDataIterator.next();
                processUseCaseLevelDbStore.put(Collections.singletonList(tuple));
            }
            LevelDBProcessUseCaseDataIterator.close();
        }

        private void ingestProcessUseCaseDataForParquetStore() {
            parquetProcessUseCaseDataIterator = generateProcessUseCaseData("http://dm.adaproq.de/vocab/", 2000); // 102000 tuples
            processUseCaseParquetStore.put(parquetProcessUseCaseDataIterator);
            parquetProcessUseCaseDataIterator.close();
        }

        private void ingestMachineUseCaseDataForLevelDbStore() {
            LevelDBMachineUseCaseDataIterator = generateMachineUseCaseData("http://dm.adaproq.de/datamodel/ESW-M-Schraube_ABC#", 34); // 102000 tuples
            while (LevelDBMachineUseCaseDataIterator.hasNext()) {
                KvinTuple tuple = LevelDBMachineUseCaseDataIterator.next();
                machineUseCaseLevelDbStore.put(Collections.singletonList(tuple));
            }
            LevelDBMachineUseCaseDataIterator.close();
        }

        private void ingestMachineUseCaseDataForParquetStore() {
            parquetMachineUseCaseDataIterator = generateMachineUseCaseData("http://dm.adaproq.de/datamodel/ESW-M-Schraube_ABC#", 34); // 102000 tuples
            machineUseCaseParquetStore.put(parquetMachineUseCaseDataIterator);
            parquetMachineUseCaseDataIterator.close();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            try {
                processUseCaseLevelDbStore.close();
                processUseCaseParquetStore.close();
                machineUseCaseLevelDbStore.close();
                machineUseCaseParquetStore.close();
                FileUtils.deleteDirectory(new File(processUseCaseLevelDbStoreDir.getAbsolutePath()));
                FileUtils.deleteDirectory(new File(processUseCaseParquetStoreDir.getAbsolutePath()));
                FileUtils.deleteDirectory(new File(machineUseCaseLevelDbStoreDir.getAbsolutePath()));
                FileUtils.deleteDirectory(new File(machineUseCaseParquetStoreDir.getAbsolutePath()));

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public NiceIterator<KvinTuple> generateProcessUseCaseData(String context, int workPieceCount) {
            return new NiceIterator<>() {
                int currentWorkPieceCount = 0;
                Long currentTimestamp = 1653335520L;
                int currentSeqNr = 0;
                boolean isGeneratingInitialGTCWorkPieceTuples = true;
                boolean isGeneratingWorkPieceTuples = false;
                int currentPropertyCount = 0;
                int weekCount = 1;
                String[] abschnittValues = {"anfang", "mitte", "end"};
                Random random = new Random(200);

                @Override
                public boolean hasNext() {
                    return currentWorkPieceCount <= workPieceCount;
                }

                @Override
                public KvinTuple next() {
                    KvinTuple tuple;
                    if (isGeneratingInitialGTCWorkPieceTuples) {
                        tuple = generateGtcWorkPieceTuple();
                    } else {
                        isGeneratingWorkPieceTuples = true;
                        tuple = generateIndividualWorkPieceTuple();
                    }
                    return tuple;
                }

                private KvinTuple generateGtcWorkPieceTuple() {
                    isGeneratingInitialGTCWorkPieceTuples = true;
                    KvinTuple generatedTuple;
                    URI item = URIs.createURI(context + "gtc");
                    URI property = URIs.createURI(context + "workpiece");
                    URI kvinContext = Kvin.DEFAULT_CONTEXT;
                    URI value = URIs.createURI(context + "wp" + currentWorkPieceCount);

                    if (currentWorkPieceCount < workPieceCount) {
                        generatedTuple = new KvinTuple(item, property, kvinContext, currentTimestamp, currentSeqNr, value);
                        //currentTimestamp = currentTimestamp + 3600;
                        currentSeqNr++;
                        currentWorkPieceCount++;
                    } else {
                        isGeneratingInitialGTCWorkPieceTuples = false;
                        currentWorkPieceCount = 0;
                        currentSeqNr = 1;
                        generatedTuple = generateIndividualWorkPieceTuple();
                    }
                    return generatedTuple;
                }

                private KvinTuple generateIndividualWorkPieceTuple() {
                    KvinTuple tuple;
                    if (currentPropertyCount < 50) {
                        if (currentPropertyCount == 0) {
                            tuple = getAbschnittTuple();
                        } else {
                            tuple = new KvinTuple(
                                    URIs.createURI(context + "wp" + currentWorkPieceCount),
                                    URIs.createURI(context + "property" + currentPropertyCount),
                                    Kvin.DEFAULT_CONTEXT,
                                    currentTimestamp,
                                    0,
                                    random.nextFloat());
                        }
                    } else {
                        currentPropertyCount = 0;
                        currentWorkPieceCount++;
                        if (currentWorkPieceCount % 100 == 0) {
                            currentTimestamp = currentTimestamp + (604800 * weekCount);
                            weekCount++;
                        }
                        tuple = getAbschnittTuple();
                    }
                    currentPropertyCount++;
                    return tuple;
                }

                private KvinTuple getAbschnittTuple() {
                    return new KvinTuple(
                            URIs.createURI(context + "wp" + currentWorkPieceCount),
                            URIs.createURI(context + "abschnitt"),
                            Kvin.DEFAULT_CONTEXT,
                            currentTimestamp,
                            0,
                            abschnittValues[random.nextInt(abschnittValues.length)]);
                }

                @Override
                public void close() {
                    super.close();
                }
            };
        }

        public NiceIterator<KvinTuple> generateMachineUseCaseData(String context, int partCount) {
            return new NiceIterator<>() {
                int currentPartCount = 0;
                Long currentTimestamp = 1653335520L;
                int currentSeqNr = 0;
                int currentPropertyCount = 0;
                int weekCount = 1, channelCount = 0;
                String currentChannelType = ".Angle";
                Random random = new Random(200);

                @Override
                public boolean hasNext() {
                    return currentPartCount < partCount;
                }

                @Override
                public KvinTuple next() {
                    return generatePartTuple();
                }

                private KvinTuple generatePartTuple() {
                    KvinTuple tuple;
                    if (currentPropertyCount > 3000) {
                        currentPropertyCount = 0;
                        channelCount = 0;
                        currentPartCount++;
                        if (currentPartCount % 5 == 0) {
                            currentTimestamp = currentTimestamp + (604800 * weekCount);
                            weekCount++;
                        }
                    }
                    tuple = generateTuple();
                    currentPropertyCount++;
                    return tuple;
                }

                private KvinTuple generateTuple() {
                    String property;
                    if (currentPropertyCount % 500 == 0) {
                        channelCount++;
                    }

                    if (currentPropertyCount % 250 == 0) {
                        if (currentChannelType.equals(".Angle")) {
                            currentChannelType = ".Force";
                        } else {
                            currentChannelType = ".Angle";
                        }
                    }

                    property = context + "ESW-M-Presskraefte_1.Channel" + channelCount + currentChannelType;

                    return new KvinTuple(
                            URIs.createURI("part:" + currentPartCount),
                            URIs.createURI(property),
                            Kvin.DEFAULT_CONTEXT,
                            currentTimestamp,
                            currentPropertyCount,
                            random.nextFloat());
                }

                @Override
                public void close() {
                    super.close();
                }
            };
        }
    }

}

