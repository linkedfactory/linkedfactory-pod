package io.github.linkedfactory.kvin.kvinParquet;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinListener;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.kvin.util.AggregatingIterator;
import io.github.linkedfactory.kvin.util.Values;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.*;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class KvinParquet implements Kvin {
    final int ROW_GROUP_SIZE = 5242880;  // 5 MB
    final int PAGE_SIZE = 8192; // 8 KB
    final int DICT_PAGE_SIZE = 3145728; // 3 MB
    final int PAGE_ROW_COUNT_LIMIT = 30000;
    int idCounter = 0;

    // data file schema
    Schema kvinTupleSchema = SchemaBuilder.record("KvinTupleInternal").namespace("io.github.linkedfactory.kvin.kvinParquet.KvinParquet").fields()
            .name("id").type().nullable().intType().noDefault()
            .name("time").type().longType().noDefault()
            .name("seqNr").type().intType().intDefault(0)
            .name("value_int").type().nullable().intType().noDefault()
            .name("value_long").type().nullable().longType().noDefault()
            .name("value_float").type().nullable().floatType().noDefault()
            .name("value_double").type().nullable().doubleType().noDefault()
            .name("value_string").type().nullable().stringType().noDefault()
            .name("value_bool").type().nullable().intType().noDefault()
            .name("value_object").type().nullable().bytesType().noDefault().endRecord();

    // mapping file schema
    Schema mappingSchema = SchemaBuilder.record("Mapping").namespace("io.github.linkedfactory.kvin.kvinParquet.KvinParquet").fields()
            .name("id").type().intType().noDefault()
            .name("item").type().stringType().noDefault()
            .name("property").type().stringType().noDefault()
            .name("context").type().stringType().noDefault().endRecord();

    @Override
    public boolean addListener(KvinListener listener) {
        return false;
    }

    @Override
    public boolean removeListener(KvinListener listener) {
        return false;
    }

    @Override
    public void put(KvinTuple... tuples) {
        this.put(Arrays.asList(tuples));
    }

    @Override
    public void put(Iterable<KvinTuple> tuples) {
        try {
            putInternal(tuples);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void putInternal(Iterable<KvinTuple> tuples) throws IOException {
        // data writer
        Path dataFile = null;
        ParquetWriter<KvinTupleInternal> parquetDataWriter = null;

        // mapping writer
        Path mappingFile = null;
        ParquetWriter<Mapping> parquetMappingWriter = null;

        Long nextChunkTimestamp = null;
        Calendar prevDate = null;
        int fileMin = 1;
        int folderMin = 1;

        for (KvinTuple tuple : tuples) {
            KvinTupleInternal internalTuple = new KvinTupleInternal();

            if (nextChunkTimestamp == null) nextChunkTimestamp = getNextChunkTimestamp(tuple.time);
            if (dataFile == null) {
                dataFile = new Path("./target/archive/" + getDate(tuple.time).get(Calendar.YEAR), "temp.parquet");
                parquetDataWriter = getParquetDataWriter(dataFile);
            }
            if (mappingFile == null) {
                mappingFile = new Path("./target/archive/" + getDate(tuple.time).get(Calendar.YEAR), "data.mapping.parquet");
                parquetMappingWriter = getParquetMappingWriter(mappingFile);
            }

            if (tuple.time >= nextChunkTimestamp) {
                //renaming existing data file with max id
                renameFile(dataFile, fileMin, idCounter);
                parquetDataWriter.close();

                fileMin = idCounter + 1;
                nextChunkTimestamp = getNextChunkTimestamp(tuple.time);

                if (prevDate.get(Calendar.YEAR) != getDate(tuple.time).get(Calendar.YEAR)) {
                    renameFolder(dataFile, folderMin, idCounter, prevDate.get(Calendar.YEAR));
                    folderMin = idCounter + 1;

                    parquetMappingWriter.close();
                    mappingFile = new Path("./target/archive/" + getDate(tuple.time).get(Calendar.YEAR), "data.mapping.parquet");
                    parquetMappingWriter = getParquetMappingWriter(mappingFile);
                }

                dataFile = new Path("./target/archive/" + getDate(tuple.time).get(Calendar.YEAR), "temp.parquet");
                parquetDataWriter = getParquetDataWriter(dataFile);
            }

            internalTuple.setId(generateId());
            internalTuple.setTime(tuple.time);
            internalTuple.setSeqNr(tuple.seqNr);

            internalTuple.setValue_int(tuple.value instanceof Integer ? (int) tuple.value : null);
            internalTuple.setValue_long(tuple.value instanceof Long ? (long) tuple.value : null);
            internalTuple.setValue_float(tuple.value instanceof Float ? (float) tuple.value : null);
            internalTuple.setValue_double(tuple.value instanceof Double ? (double) tuple.value : null);
            internalTuple.setValue_string(tuple.value instanceof String ? (String) tuple.value : null);
            internalTuple.setValue_bool(tuple.value instanceof Boolean ? (Boolean) tuple.value ? 1 : 0 : null);
            if (tuple.value instanceof Record || tuple.value instanceof URI || tuple.value instanceof BigInteger || tuple.value instanceof BigDecimal || tuple.value instanceof Short) {
                internalTuple.setValue_object(encodeRecord(tuple.value));
            } else {
                internalTuple.setValue_object(null);
            }
            // generating mapping
            Mapping mapping = new Mapping();
            mapping.setId(internalTuple.getId());
            mapping.setItem(tuple.item.toString());
            mapping.setProperty(tuple.property.toString());
            mapping.setContext(tuple.context.toString());
            // writing mapping and data
            parquetMappingWriter.write(mapping);
            parquetDataWriter.write(internalTuple);
            prevDate = getDate(tuple.time);

        }
        renameFile(dataFile, fileMin, idCounter);
        renameFolder(dataFile, folderMin, idCounter, prevDate.get(Calendar.YEAR));
        parquetMappingWriter.close();
        parquetDataWriter.close();
    }

    private ParquetWriter<KvinTupleInternal> getParquetDataWriter(Path dataFile) throws IOException {
        Configuration writerConf = new Configuration();
        writerConf.setInt("parquet.zstd.compressionLevel", 12);
        return AvroParquetWriter.<KvinTupleInternal>builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
                .withSchema(kvinTupleSchema)
                .withConf(writerConf)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(PAGE_SIZE)
                .withPageRowCountLimit(PAGE_ROW_COUNT_LIMIT)
                .withDictionaryPageSize(DICT_PAGE_SIZE)
                .withDataModel(ReflectData.get())
                .build();
    }

    private ParquetWriter<Mapping> getParquetMappingWriter(Path dataFile) throws IOException {
        Configuration writerConf = new Configuration();
        writerConf.setInt("parquet.zstd.compressionLevel", 12);
        return AvroParquetWriter.<Mapping>builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
                .withSchema(mappingSchema)
                .withConf(writerConf)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(PAGE_SIZE)
                .withPageRowCountLimit(PAGE_ROW_COUNT_LIMIT)
                .withDictionaryPageSize(DICT_PAGE_SIZE)
                .withDataModel(ReflectData.get())
                .build();
    }

    private long getNextChunkTimestamp(long currentTimestamp) {
        return currentTimestamp + 604800;
    }

    private void renameFile(Path file, int min, int max) throws IOException {
        java.nio.file.Path currentFile = Paths.get(file.toString());
        Files.move(currentFile, currentFile.resolveSibling(min + "_" + max + "_" + ".parquet"));
    }

    private void renameFolder(Path file, int min, int max, int year) throws IOException {
        java.nio.file.Path currentFolder = Paths.get(file.getParent().toString());
        Files.move(currentFolder, currentFolder.resolveSibling(min + "_" + max + "_" + year));
    }

    private Calendar getDate(long timestamp) {
        Timestamp ts = new Timestamp(timestamp * 1000);
        Date date = new java.sql.Date(ts.getTime());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar;
    }

    private int generateId() {
        return ++idCounter;
    }

    private ArrayList<Mapping> getIdMapping(URI item, URI property, URI context) throws IOException {
        ArrayList<Mapping> mappings = new ArrayList<>();
        File[] folders = new File("./target/archive/").listFiles();
        Arrays.sort(folders, (file1, file2) -> {
            Integer firstFileYear = Integer.valueOf(file1.getName().split("_")[2]);
            Integer secondFileYear = Integer.valueOf(file2.getName().split("_")[2]);
            return firstFileYear.compareTo(secondFileYear);
        });
        int prevMappingCount = 0;
        boolean checkForOverlappingRead = false;

        for (File folder : folders) {
            Path mappingFile = new Path(folder.getPath() + "/data.mapping.parquet");
            FilterPredicate filter = null;
            int currentMappingCount = 0;
            if (item != null && property != null) {
                filter = and(eq(FilterApi.binaryColumn("item"), Binary.fromString(item.toString())), eq(FilterApi.binaryColumn("property"), Binary.fromString(property.toString())));
            } else if (item != null) {
                filter = eq(FilterApi.binaryColumn("item"), Binary.fromString(item.toString()));
            }
            try (ParquetReader<Mapping> reader = AvroParquetReader.<Mapping>builder(HadoopInputFile.fromPath(mappingFile, new Configuration()))
                    .withDataModel(new ReflectData(Mapping.class.getClassLoader()))
                    .withFilter(FilterCompat.get(filter))
                    .build()) {

                Mapping mapping;
                while ((mapping = reader.read()) != null) {
                    currentMappingCount++;
                    mappings.add(mapping);
                }
                if (currentMappingCount > 0) checkForOverlappingRead = true;
                if (prevMappingCount == 0 && currentMappingCount == 0 && checkForOverlappingRead) break;
                prevMappingCount = currentMappingCount;
            }
        }

        return mappings;
    }

    private FilterPredicate generateFetchFilter(ArrayList<Mapping> mappings) {
        int mappingSize = mappings.size();
        FilterPredicate filter = null;
        if (mappingSize > 0) {
            if (mappingSize == 1) {
                filter = or(eq(FilterApi.intColumn("id"), mappings.get(0).getId()), eq(FilterApi.intColumn("id"), -1));
            } else if (mappingSize == 2) {
                filter = or(eq(FilterApi.intColumn("id"), mappings.get(0).getId()), eq(FilterApi.intColumn("id"), mappings.get(1).getId()));
            } else {
                filter = generateFilterPredicates(mappings, 0);
            }
        }
        return filter;
    }

    private FilterPredicate generateFilterPredicates(ArrayList<Mapping> mappings, int startCount) {
        FilterPredicate predicate = eq(FilterApi.intColumn("id"), -1);
        if (startCount < mappings.size()) {
            predicate = FilterApi.or(eq(FilterApi.intColumn("id"), mappings.get(startCount).getId()), generateFilterPredicates(mappings, ++startCount));
        }
        return predicate;
    }

    private byte[] encodeRecord(Object record) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        if (record instanceof Record) {
            Record r = (Record) record;
            byteArrayOutputStream.write("O".getBytes(StandardCharsets.UTF_8));
            byte[] propertyBytes = r.getProperty().toString().getBytes();
            byteArrayOutputStream.write((byte) propertyBytes.length);
            byteArrayOutputStream.write(propertyBytes);
            byteArrayOutputStream.write(encodeRecord(r.getValue()));
        } else if (record instanceof URI) {
            URI uri = (URI) record;
            byte[] uriIndicatorBytes = "R".getBytes(StandardCharsets.UTF_8);
            byte[] uriBytes = new byte[uri.toString().getBytes().length + 1];
            uriBytes[0] = (byte) uri.toString().getBytes().length;
            System.arraycopy(uri.toString().getBytes(), 0, uriBytes, 1, uriBytes.length - 1);

            byte[] combinedBytes = new byte[uriIndicatorBytes.length + uriBytes.length];
            System.arraycopy(uriIndicatorBytes, 0, combinedBytes, 0, uriIndicatorBytes.length);
            System.arraycopy(uriBytes, 0, combinedBytes, uriIndicatorBytes.length, uriBytes.length);
            return combinedBytes;
        } else {
            return Values.encode(record);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private Object decodeRecord(byte[] data) {
        Record r = null;
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
            char type = (char) byteArrayInputStream.read();
            if (type == 'O') {
                int propertyLength = byteArrayInputStream.read();
                String property = new String(byteArrayInputStream.readNBytes(propertyLength), StandardCharsets.UTF_8);
                var value = decodeRecord(byteArrayInputStream.readAllBytes());
                if (r != null) {
                    r.append(new Record(URIs.createURI(property), value));
                } else {
                    r = new Record(URIs.createURI(property), value);
                }
            } else if (type == 'R') {
                int uriLength = byteArrayInputStream.read();
                String uri = new String(byteArrayInputStream.readNBytes(uriLength), StandardCharsets.UTF_8);
                return URIs.createURI(uri);
            } else {
                return Values.decode(data);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return r;
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
        return fetchInternal(item, property, context, null, null, limit, null, null);
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
        IExtendedIterator<KvinTuple> internalResult = fetchInternal(item, property, context, end, begin, limit, interval, op);
        if (op != null) {
            internalResult = new AggregatingIterator<>(internalResult, interval, op.trim().toLowerCase(), limit) {
                @Override
                protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
                    return new KvinTuple(item, property, context, time, seqNr, value);
                }
            };
        }
        return internalResult;
    }

    private IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, Long end, Long begin, Long limit, Long interval, String op) {

        try {
            // filters
            ArrayList<Mapping> idMappings = getIdMapping(item, property, context);

            if (idMappings.size() == 0) {
                return NiceIterator.emptyIterator();
            }

            FilterPredicate filter = generateFetchFilter(idMappings);
            ArrayList<Path> dataFiles = getFilePath(idMappings);
            ArrayList<ParquetReader<KvinTupleInternal>> readers = new ArrayList<>();
            // data reader
            for (Path path : dataFiles) {
                readers.add(AvroParquetReader.<KvinTupleInternal>builder(HadoopInputFile.fromPath(path, new Configuration()))
                        .withDataModel(new ReflectData(KvinTupleInternal.class.getClassLoader()))
                        .withFilter(FilterCompat.get(filter))
                        .build());
            }
            return new NiceIterator<>() {
                KvinTupleInternal internalTuple;
                ParquetReader<KvinTupleInternal> reader = readers.get(0);
                HashMap<String, Integer> itemPropertyCount = new HashMap<>();
                int propertyCount = 0, readerCount = 0;
                String currentProperty, previousProperty;

                @Override
                public boolean hasNext() {
                    try {
                        if (itemPropertyCount.size() > 0) {
                            if (itemPropertyCount.get(currentProperty) >= limit && limit != 0) {
                                currentProperty = idMappings.get(propertyCount).getProperty();
                                previousProperty = currentProperty;

                                while ((internalTuple = reader.read()) != null && propertyCount < idMappings.size() - 1) {
                                    propertyCount++;
                                    if (!previousProperty.equals(idMappings.get(propertyCount).getProperty())) {
                                        break;
                                    }
                                }
                            }
                        }
                        internalTuple = reader.read();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    if (internalTuple == null && readerCount >= readers.size() - 1) {
                        return false;
                    } else if (internalTuple == null && readerCount <= readers.size() - 1 && itemPropertyCount.get(currentProperty) >= limit && limit != 0) {
                        readerCount++;
                        reader = readers.get(readerCount);
                        return hasNext();
                    } else if (internalTuple == null && readerCount <= readers.size() - 1) {
                        readerCount++;
                        reader = readers.get(readerCount);
                        try {
                            internalTuple = reader.read();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return true;
                }

                @Override
                public KvinTuple next() {
                    if (internalTuple != null) {
                        KvinTuple tuple = internalTupleToKvinTuple(internalTuple);
                        propertyCount++;
                        return tuple;
                    } else {
                        return null;
                    }
                }

                private KvinTuple internalTupleToKvinTuple(KvinTupleInternal internalTuple) {
                    Object value = null;
                    if (internalTuple.value_int != null) {
                        value = internalTuple.value_int;
                    } else if (internalTuple.value_long != null) {
                        value = internalTuple.value_long;
                    } else if (internalTuple.value_float != null) {
                        value = internalTuple.value_float;
                    } else if (internalTuple.value_double != null) {
                        value = internalTuple.value_double;
                    } else if (internalTuple.value_string != null) {
                        value = internalTuple.value_string;
                    } else if (internalTuple.value_bool != null) {
                        value = internalTuple.value_bool == 1;
                    } else if (internalTuple.value_object != null) {
                        value = decodeRecord(internalTuple.value_object);
                    }

                    if (currentProperty == null) {
                        currentProperty = idMappings.get(propertyCount).getProperty();
                        previousProperty = currentProperty;
                    } else if (!idMappings.get(propertyCount).getProperty().equals(previousProperty)) {
                        currentProperty = idMappings.get(propertyCount).getProperty();
                        previousProperty = idMappings.get(propertyCount).getProperty();
                        itemPropertyCount.clear();
                    }

                    if (itemPropertyCount.containsKey(idMappings.get(propertyCount).getProperty())) {
                        String property = idMappings.get(propertyCount).getProperty();
                        Integer count = itemPropertyCount.get(property) + 1;
                        itemPropertyCount.put(property, count);
                    } else {
                        itemPropertyCount.put(idMappings.get(propertyCount).getProperty(), 1);
                    }

                    return new KvinTuple(URIs.createURI(idMappings.get(propertyCount).getItem()), URIs.createURI(idMappings.get(propertyCount).getProperty()), URIs.createURI(idMappings.get(propertyCount).getContext()), internalTuple.time, internalTuple.seqNr, value);

                }

                @Override
                public void close() {
                    super.close();
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ArrayList<Path> getFilePath(ArrayList<Mapping> idMappings) {

        File archiveFolder = new File("./target/archive");
        File[] yearWiseFolders = archiveFolder.listFiles();
        ArrayList<Path> matchedFiles = new ArrayList<>();

        if (yearWiseFolders != null) {
            for (Mapping mapping : idMappings) {
                for (File folder : yearWiseFolders) {
                    try {
                        String[] FolderIdMinMaxData = folder.getName().split("_");
                        int folderMin = Integer.valueOf(FolderIdMinMaxData[0]);
                        int folderMax = Integer.valueOf(FolderIdMinMaxData[1]);
                        if (mapping.getId() >= folderMin && mapping.getId() <= folderMax) {
                            for (File file : new File(folder.getPath()).listFiles()) {
                                try {
                                    String[] fileIdMinMaxData = file.getName().split("_");
                                    int fileMin = Integer.valueOf(fileIdMinMaxData[0]);
                                    int fileMax = Integer.valueOf(fileIdMinMaxData[1]);
                                    if (mapping.getId() >= fileMin && mapping.getId() <= fileMax) {
                                        Path path = new Path(file.getPath());
                                        if (!matchedFiles.contains(path)) matchedFiles.add(path);
                                        break;
                                    }
                                } catch (RuntimeException exception) {
                                }
                            }
                        }
                    } catch (RuntimeException exception) {
                    }
                }
            }
        }
        return matchedFiles;
    }

    @Override
    public long delete(URI item, URI property, URI context, long end, long begin) {
        return 0;
    }

    @Override
    public boolean delete(URI item) {
        return false;
    }

    @Override
    public IExtendedIterator<URI> descendants(URI item) {
        return null;
    }

    @Override
    public IExtendedIterator<URI> descendants(URI item, long limit) {
        return null;
    }

    @Override
    public IExtendedIterator<URI> properties(URI item) {

        try {
            // filters
            ArrayList<Mapping> idMappings = getIdMapping(item, null, null);
            if (idMappings.size() == 0) {
                return NiceIterator.emptyIterator();
            }
            FilterPredicate filter = generateFilterPredicates(idMappings, 0);
            ArrayList<Path> dataFiles = getFilePath(idMappings);
            ArrayList<ParquetReader<KvinTupleInternal>> readers = new ArrayList<>();

            // data reader
            for (Path path : dataFiles) {
                readers.add(AvroParquetReader.<KvinTupleInternal>builder(HadoopInputFile.fromPath(path, new Configuration()))
                        .withDataModel(new ReflectData(KvinTupleInternal.class.getClassLoader()))
                        .withFilter(FilterCompat.get(filter))
                        .build());
            }

            return new NiceIterator<>() {
                KvinTupleInternal internalTuple;
                ParquetReader<KvinTupleInternal> reader = readers.get(0);
                int propertyCount = 0, readerCount = 0;

                @Override
                public boolean hasNext() {
                    try {
                        internalTuple = reader.read();
                        if (internalTuple == null && readerCount >= readers.size() - 1) {
                            return false;
                        } else if (internalTuple == null && readerCount <= readers.size() - 1) {
                            readerCount++;
                            reader = readers.get(readerCount);
                            try {
                                internalTuple = reader.read();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return true;
                }

                @Override
                public URI next() {
                    URI property = getKvinTupleProperty();
                    propertyCount++;
                    return property;
                }

                private URI getKvinTupleProperty() {
                    return URIs.createURI(idMappings.get(propertyCount).getProperty());
                }

                @Override
                public void close() {
                    super.close();
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long approximateSize(URI item, URI property, URI context, long end, long begin) {
        return 0;
    }

    @Override
    public void close() {

    }

    static class KvinTupleInternal {
        int id;
        Long time;
        Integer seqNr;
        Integer value_int;
        Long value_long;
        Float value_float;
        Double value_double;
        String value_string;
        Integer value_bool;
        byte[] value_object;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public int getSeqNr() {
            return seqNr;
        }

        public void setSeqNr(int seqNr) {
            this.seqNr = seqNr;
        }

        public Integer getValue_int() {
            return value_int;
        }

        public void setValue_int(Integer value_int) {
            this.value_int = value_int;
        }

        public Long getValue_long() {
            return value_long;
        }

        public void setValue_long(Long value_long) {
            this.value_long = value_long;
        }

        public Float getValue_float() {
            return value_float;
        }

        public void setValue_float(Float value_float) {
            this.value_float = value_float;
        }

        public Double getValue_double() {
            return value_double;
        }

        public void setValue_double(Double value_double) {
            this.value_double = value_double;
        }

        public String getValue_string() {
            return value_string;
        }

        public void setValue_string(String value_string) {
            this.value_string = value_string;
        }

        public byte[] getValue_object() {
            return value_object;
        }

        public void setValue_object(byte[] value_object) {
            this.value_object = value_object;
        }

        public Integer getValue_bool() {
            return value_bool;
        }

        public void setValue_bool(Integer value_bool) {
            this.value_bool = value_bool;
        }

    }

    static class Mapping {
        Integer id;
        String item;
        String property;
        String context;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getItem() {
            return item;
        }

        public void setItem(String item) {
            this.item = item;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }

        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }


    }
}
