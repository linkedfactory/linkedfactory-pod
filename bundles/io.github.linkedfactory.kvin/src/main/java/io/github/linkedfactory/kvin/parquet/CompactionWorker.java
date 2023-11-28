package io.github.linkedfactory.kvin.parquet;

import net.enilink.commons.util.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static io.github.linkedfactory.kvin.parquet.ParquetHelpers.*;

public class CompactionWorker implements Runnable {
    String archiveLocation;
    KvinParquet kvinParquet;
    int dataFileCompactionTrigger = 2, mappingFileCompactionTrigger = 3;

    public CompactionWorker(String archiveLocation, KvinParquet kvinParquet) {
        this.archiveLocation = archiveLocation;
        this.kvinParquet = kvinParquet;
    }

    @Override
    public void run() {
        performMappingFileCompaction();
        performDataFileCompaction();
    }

    private void performMappingFileCompaction() {
        try {
            Map<String, List<Pair<String, Integer>>> mappingFiles = getMappingFiles(Paths.get(archiveLocation + "/metadata"));
            if (mappingFiles.get("itemMapping").size() >= mappingFileCompactionTrigger) {
                kvinParquet.readLock.lock();
                Path compactionFolder = new Path(archiveLocation + "/metadata", ".compaction");
                generateCompactedMappingFiles(mappingFiles, compactionFolder);
                kvinParquet.readLock.unlock();

                kvinParquet.writeLock.lock();
                clearCache();
                replaceCompactedMappingFiles(archiveLocation, compactionFolder);
                kvinParquet.writeLock.unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void performDataFileCompaction() {
        ArrayList<File> weekFolders = getCompactionEligibleWeekFolders();
        for (File weekFolder : weekFolders) {
            try {
                compactDataFiles(weekFolder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ArrayList<File> getCompactionEligibleWeekFolders() {
        ArrayList<File> weekFolderList = new ArrayList<>();
        File[] yearFolders = new File(archiveLocation).listFiles((file, s) -> !s.startsWith("metadata"));
        for (File yearFolder : yearFolders) {
            File[] weekFolders = yearFolder.listFiles();
            for (File weekFolder : weekFolders) {
                File[] dataFiles = weekFolder.listFiles((file, s) -> s.endsWith("parquet"));
                if (dataFiles.length >= dataFileCompactionTrigger) {
                    weekFolderList.add(weekFolder);
                }
            }
        }
        return weekFolderList;
    }

    private void generateCompactedMappingFiles(Map<String, List<Pair<String, Integer>>> mappingFiles, Path compactionFolder) throws IOException {
        for (Map.Entry<String, List<Pair<String, Integer>>> mapping : mappingFiles.entrySet()) {
            Path compactedFile = new Path(compactionFolder, mapping.getKey() + "__1.parquet");
            ParquetWriter<Object> compactedFileWriter = getParquetMappingWriter(compactedFile);

            for (Pair<String, Integer> file : mapping.getValue()) {
                ParquetReader<IdMapping> mappingFileReader = getParquetMappingReader(
                        HadoopInputFile.fromPath(new Path(archiveLocation + "metadata/" + file.getFirst()), new Configuration()));
                IdMapping idMapping = mappingFileReader.read();
                while (idMapping != null) {
                    compactedFileWriter.write(idMapping);
                    idMapping = mappingFileReader.read();
                }
                mappingFileReader.close();
            }
            compactedFileWriter.close();
        }
    }

    private ParquetReader<IdMapping> getParquetMappingReader(HadoopInputFile file) throws IOException {
        return AvroParquetReader.<IdMapping>builder(file)
                .withDataModel(reflectData)
                .useStatsFilter()
                .build();
    }

    private ParquetReader<KvinTupleInternal> getParquetDataReader(HadoopInputFile file) throws IOException {
        return AvroParquetReader.<KvinTupleInternal>builder(file)
                .withDataModel(reflectData)
                .useStatsFilter()
                .build();
    }

    private void replaceCompactedMappingFiles(String archiveLocation, Path compactionFolder) throws IOException {
        // deleting existing files
        File[] existingFiles = new File(archiveLocation + "metadata").listFiles();
        for (File existingFile : existingFiles) {
            if (!existingFile.isDirectory()) {
                existingFile.delete();
            }
        }

        // moving files
        File[] compactedFiles = new File(compactionFolder.toString()).listFiles();
        for (File compactedFile : compactedFiles) {
            Files.move(Paths.get(compactedFile.getPath()), Paths.get(archiveLocation.toString() + "metadata/" + compactedFile.getName()));
        }

        // deleting compaction folder
        FileUtils.deleteDirectory(new File(compactionFolder.toString()));
    }

    private void replaceCompactedDataFiles(File weekFolder, Path compactionFolder) throws IOException {
        // deleting existing files
        File[] existingFiles = weekFolder.listFiles();
        for (File existingFile : existingFiles) {
            if (!existingFile.isDirectory()) {
                existingFile.delete();
            }
        }

        // moving files
        File[] compactedFiles = new File(compactionFolder.toString()).listFiles();
        for (File compactedFile : compactedFiles) {
            Files.move(Paths.get(compactedFile.getPath()), Paths.get(weekFolder.getPath(), compactedFile.getName()));
        }

        // deleting compaction folder
        FileUtils.deleteDirectory(new File(compactionFolder.toString()));
    }

    private void compactDataFiles(File weekFolder) throws IOException {
        kvinParquet.readLock.lock();
        List<java.nio.file.Path> dataFiles = Files.walk(weekFolder.toPath(), 1)
                .skip(1)
                .filter(path -> path.getFileName().toString().startsWith("data_"))
                .collect(Collectors.toList());

        Path compactionFolder = new Path(weekFolder.getPath(), ".compaction");
        Path compactionFile = new Path(compactionFolder, "data__1.parquet");
        ParquetWriter<KvinTupleInternal> compactionFileWriter = getParquetDataWriter(compactionFile);

        PriorityQueue<Pair<KvinTupleInternal, ParquetReader<KvinTupleInternal>>> nextTuples =
                new PriorityQueue<>(Comparator.comparing(Pair::getFirst));
        for (java.nio.file.Path dataFile : dataFiles) {
            ParquetReader<KvinTupleInternal> reader = getParquetDataReader(
                    HadoopInputFile.fromPath(new Path(dataFile.toString()), new Configuration()));
            KvinTupleInternal tuple = reader.read();
            if (tuple != null) {
                nextTuples.add(new Pair<>(tuple, reader));
            } else {
                try {
                    reader.close();
                } catch (IOException e) {
                }
            }
        }

        while (! nextTuples.isEmpty()) {
            var pair = nextTuples.poll();
            compactionFileWriter.write(pair.getFirst());
            KvinTupleInternal tuple = pair.getSecond().read();
            if (tuple != null) {
                nextTuples.add(new Pair<>(tuple, pair.getSecond()));
            } else {
                try {
                    pair.getSecond().close();
                } catch (IOException e) {
                }
            }
        }

        compactionFileWriter.close();
        kvinParquet.readLock.unlock();

        kvinParquet.writeLock.lock();
        clearCache();
        replaceCompactedDataFiles(weekFolder, compactionFolder);
        kvinParquet.writeLock.unlock();
    }

    private void clearCache() {
        synchronized (kvinParquet.inputFileCache) {
            kvinParquet.inputFileCache.clear();
        }
    }
}
