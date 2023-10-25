package io.github.linkedfactory.kvin.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static io.github.linkedfactory.kvin.parquet.KvinParquet.reflectData;

public class CompactionWorker implements Runnable {
    String archiveLocation;
    KvinParquet kvinParquet;
    int dataFileCompactionTrigger = 2, mappingFileCompactionTrigger = 3;

    Schema idMappingSchema = SchemaBuilder.record("SimpleMapping").namespace(KvinParquet.class.getName()).fields()
            .name("id").type().longType().noDefault()
            .name("value").type().stringType().noDefault().endRecord();

    Schema kvinTupleSchema = SchemaBuilder.record("KvinTupleInternal").namespace(KvinParquet.class.getName()).fields()
            .name("id").type().nullable().bytesType().noDefault()
            .name("time").type().longType().noDefault()
            .name("seqNr").type().intType().intDefault(0)
            .name("valueInt").type().nullable().intType().noDefault()
            .name("valueLong").type().nullable().longType().noDefault()
            .name("valueFloat").type().nullable().floatType().noDefault()
            .name("valueDouble").type().nullable().doubleType().noDefault()
            .name("valueString").type().nullable().stringType().noDefault()
            .name("valueBool").type().nullable().intType().noDefault()
            .name("valueObject").type().nullable().bytesType().noDefault().endRecord();

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
            HashMap<String, ArrayList<File>> mappingFiles = getMappingFiles();
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

    private HashMap<String, ArrayList<File>> getMappingFiles() {
        // returns count of itemMappingFiles
        Path metadataFolder = new Path(archiveLocation, "metadata");
        File[] mappingFiles = new File(metadataFolder.toString()).listFiles();
        HashMap<String, ArrayList<File>> fileMap = new HashMap<>();
        ArrayList<File> itemMapping = new ArrayList<>();
        ArrayList<File> propertyMapping = new ArrayList<>();
        ArrayList<File> contextMapping = new ArrayList<>();

        if (mappingFiles != null) {
            for (File mappingFile : mappingFiles) {
                if (mappingFile.getName().endsWith(".parquet")) {
                    if (mappingFile.getName().startsWith("item")) {
                        itemMapping.add(mappingFile);
                    } else if (mappingFile.getName().startsWith("property")) {
                        propertyMapping.add(mappingFile);
                    } else if (mappingFile.getName().startsWith("context")) {
                        contextMapping.add(mappingFile);
                    }
                }
            }
        }
        itemMapping.sort(Comparator.naturalOrder());
        propertyMapping.sort(Comparator.naturalOrder());
        contextMapping.sort(Comparator.naturalOrder());

        fileMap.put("itemMapping", itemMapping);
        fileMap.put("propertyMapping", propertyMapping);
        fileMap.put("contextMapping", contextMapping);
        return fileMap;
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

    private void generateCompactedMappingFiles(HashMap<String, ArrayList<File>> mappingFiles, Path compactionFolder) throws IOException {
        for (Map.Entry<String, ArrayList<File>> mapping : mappingFiles.entrySet()) {
            Path compactedFile = new Path(compactionFolder, mapping.getValue().get(0).getName());
            ParquetWriter<Object> compactedFileWriter = getParquetMappingWriter(compactedFile);

            for (File file : mapping.getValue()) {
                ParquetReader<KvinParquet.IdMapping> mappingFileReader = getParquetMappingReader(HadoopInputFile.fromPath(new Path(file.getPath()), new Configuration()));
                KvinParquet.IdMapping idMapping = mappingFileReader.read();

                while (idMapping != null) {
                    compactedFileWriter.write(idMapping);
                    idMapping = mappingFileReader.read();
                }
                mappingFileReader.close();
            }
            compactedFileWriter.close();
        }
    }

    private ParquetReader<KvinParquet.IdMapping> getParquetMappingReader(HadoopInputFile file) throws IOException {
        return AvroParquetReader.<KvinParquet.IdMapping>builder(file)
                .withDataModel(reflectData)
                .useStatsFilter()
                .build();
    }

    private ParquetReader<KvinParquet.KvinTupleInternal> getParquetDataReader(HadoopInputFile file) throws IOException {
        return AvroParquetReader.<KvinParquet.KvinTupleInternal>builder(file)
                .withDataModel(reflectData)
                .useStatsFilter()
                .build();
    }

    private ParquetWriter<Object> getParquetMappingWriter(Path dataFile) throws IOException {
        Configuration writerConf = new Configuration();
        writerConf.setInt("parquet.zstd.compressionLevel", 12);
        return AvroParquetWriter.builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
                .withSchema(idMappingSchema)
                .withDataModel(reflectData)
                .withConf(writerConf)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                //.withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(1048576)
                .withPageSize(8192)
                .withDictionaryPageSize(1048576)
                .build();
    }

    private ParquetWriter<KvinParquet.KvinTupleInternal> getParquetDataWriter(Path dataFile) throws IOException {
        Configuration writerConf = new Configuration();
        writerConf.setInt("parquet.zstd.compressionLevel", 12);
        return AvroParquetWriter.<KvinParquet.KvinTupleInternal>builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
                .withSchema(kvinTupleSchema)
                .withDataModel(reflectData)
                .withConf(writerConf)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                //.withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(1048576)
                .withPageSize(8192)
                .withDictionaryPageSize(1048576)
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
            Files.move(Paths.get(compactedFile.getPath()), Paths.get(weekFolder.getPath() + "/" + compactedFile.getName()));
        }

        // deleting compaction folder
        FileUtils.deleteDirectory(new File(compactionFolder.toString()));
    }

    private void compactDataFiles(File weekFolder) throws IOException {
        kvinParquet.readLock.lock();
        List<java.nio.file.Path> dataFiles = Files.walk(weekFolder.toPath(), 1)
                .skip(1)
                .filter(path -> path.getFileName().toString().startsWith("data_"))
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());

        long compactedFileStartTime = Long.parseLong(dataFiles.get(0).getFileName().toString().split("_")[1]);
        long compactedFileEndTime = Long.parseLong(dataFiles.get(dataFiles.size() - 1).getFileName().toString().split("_")[2].split(".parquet")[0]);
        Path compactionFolder = new Path(weekFolder.getPath(), ".compaction");
        Path compactionFile = new Path(compactionFolder, "data_" + compactedFileStartTime + "_" + compactedFileEndTime + ".parquet");
        ParquetWriter<KvinParquet.KvinTupleInternal> compactionFileWriter = getParquetDataWriter(compactionFile);

        for (java.nio.file.Path dataFile : dataFiles) {
            ParquetReader<KvinParquet.KvinTupleInternal> dataFileReader = getParquetDataReader(HadoopInputFile.fromPath(new Path(new File(dataFile.toString()).toString()), new Configuration()));
            KvinParquet.KvinTupleInternal tuple = dataFileReader.read();

            while (tuple != null) {
                compactionFileWriter.write(tuple);
                tuple = dataFileReader.read();
            }
            dataFileReader.close();
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
