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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static io.github.linkedfactory.kvin.parquet.KvinParquet.reflectData;

public class CompactionWorker implements Runnable {
    String archiveLocation;
    KvinParquet kvinParquet;
    int dataFileCompactionTrigger = 3, mappingFileCompactionTrigger = 3;

    Schema idMappingSchema = SchemaBuilder.record("SimpleMapping").namespace(KvinParquet.class.getName()).fields()
            .name("id").type().longType().noDefault()
            .name("value").type().stringType().noDefault().endRecord();

    public CompactionWorker(String archiveLocation, KvinParquet kvinParquet) {
        this.archiveLocation = archiveLocation;
        this.kvinParquet = kvinParquet;
    }

    @Override
    public void run() {
        performMappingFileCompaction();
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
                replaceCompactedMappingFiles(archiveLocation, compactionFolder);
                kvinParquet.writeLock.unlock();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void performDataFileCompaction() {

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

        // delete compaction folder
        FileUtils.deleteDirectory(new File(compactionFolder.toString()));
    }
}
