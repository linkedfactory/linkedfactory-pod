package io.github.linkedfactory.kvin.parquet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class KvinParquet implements Kvin {

    final static ReflectData reflectData = new ReflectData(KvinParquet.class.getClassLoader());
    // parquet file writer config
    static final long ROW_GROUP_SIZE = 1048576;  // 1 MB
    static final int PAGE_SIZE = 8192; // 8 KB
    static final int DICT_PAGE_SIZE = 1048576; // 1 MB
    static final int ZSTD_COMPRESSION_LEVEL = 12; // 1 - 22
    Map<Path, HadoopInputFile> inputFileCache = new HashMap<>(); // hadoop input file cache
    Cache<Long, String> propertyIdReverseLookUpCache = CacheBuilder.newBuilder().maximumSize(10000).build();
    String archiveLocation;
    // data file schema
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
    // mapping file schema
    Schema idMappingSchema = SchemaBuilder.record("SimpleMapping").namespace(KvinParquet.class.getName()).fields()
            .name("id").type().longType().noDefault()
            .name("value").type().stringType().noDefault().endRecord();
    long itemIdCounter = 0, propertyIdCounter = 0, contextIdCounter = 0; // global id counter

    // used by writer
    Map<String, Long> itemMap = new HashMap<>();
    Map<String, Long> propertyMap = new HashMap<>();
    Map<String, Long> contextMap = new HashMap<>();

    // used by reader
    Cache<URI, Long> itemIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	Cache<URI, Long> propertyIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	Cache<URI, Long> contextIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();

	static class IdMappings {
	    long itemId, propertyId, contextId;
    }

    public KvinParquet(String archiveLocation) {
        this.archiveLocation = archiveLocation;
    }

    private IdMapping fetchMappingIds(Path mappingFile, FilterPredicate filter) throws IOException {
        IdMapping id;
        HadoopInputFile inputFile = getFile(mappingFile);
        try (ParquetReader<IdMapping> reader = AvroParquetReader.<IdMapping>builder(inputFile)
                .withDataModel(reflectData)
                .useStatsFilter()
                .withFilter(FilterCompat.get(filter))
                .build()) {
            id = reader.read();
        }
        return id;
    }

    private HadoopInputFile getFile(Path path) {
        HadoopInputFile inputFile;
        synchronized (inputFileCache) {
            inputFile = inputFileCache.get(path);
            if (inputFile == null) {
                try {
                    inputFile = HadoopInputFile.fromPath(path, new Configuration());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                inputFileCache.put(path, inputFile);
            }
        }
        return inputFile;
    }

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
        Path itemMappingFile, propertyMappingFile, contextMappingFile;
        ParquetWriter<Object> itemMappingWriter = null, propertyMappingWriter = null, contextMappingWriter = null;

        //  state variables
        boolean writingToExistingYearFolder = false;
        Long nextChunkTimestamp = null;
        Calendar prevTupleDate = null;

        // initial partition key
        long initialPartitionKey = 1L;
        long weekPartitionKey = initialPartitionKey, yearPartitionKey = initialPartitionKey;

        for (KvinTuple tuple : tuples) {
            KvinTupleInternal internalTuple = new KvinTupleInternal();
            nextChunkTimestamp = initNextChunkTimestamp(nextChunkTimestamp, tuple);

            // initializing writers to data and mapping file along with the initial folders.
            if (dataFile == null) {
                int year = getDate(tuple.time).get(Calendar.YEAR);
                // new year and week folder
                if (!getExistingYears().contains(year)) {
                    dataFile = new Path(archiveLocation + getDate(tuple.time).get(Calendar.YEAR), "temp/data.parquet");
                } else {
                    // existing year and week folder
                    File existingYearFolder = getExistingYearFolder(year);
                    String existingYearFolderPath = existingYearFolder.getAbsolutePath();

                    dataFile = new Path(existingYearFolderPath, "temp/data.parquet");
                    yearPartitionKey = Long.parseLong(existingYearFolder.getName().split("_")[0]); // minOfItemIdOfAllTheWeeks; // minOfItemIdOfAllTheWeeks
                    writingToExistingYearFolder = true;
                }
                // mapping file writers init
                itemMappingFile = new Path(archiveLocation, "metadata/itemMapping.parquet");
                propertyMappingFile = new Path(archiveLocation, "metadata/propertyMapping.parquet");
                contextMappingFile = new Path(archiveLocation, "metadata/contextMapping.parquet");

                parquetDataWriter = getParquetDataWriter(dataFile);
                itemMappingWriter = getParquetMappingWriter(itemMappingFile, idMappingSchema);
                propertyMappingWriter = getParquetMappingWriter(propertyMappingFile, idMappingSchema);
                contextMappingWriter = getParquetMappingWriter(contextMappingFile, idMappingSchema);
            }

            // partitioning file on week change
            if (tuple.time >= nextChunkTimestamp) {
                // renaming current week folder with partition key name. ( at the start, while writing into the current week folder data and mapping files, the folder name is set to "temp".)
                // key: WeekMinItemPropertyContextId_WeekMaxItemPropertyContextId
                renameFolder(dataFile, weekPartitionKey, itemIdCounter);

                // updating partition key of the folder with the max itemId of the newly added week folder
                // key: YearMinItemPropertyContextId_YearMaxItemPropertyContextId
                if (writingToExistingYearFolder)
                    renameFolder(dataFile, yearPartitionKey, itemIdCounter, prevTupleDate.get(Calendar.YEAR));

                // updating new week partition id
                weekPartitionKey = itemIdCounter;
                if (!itemMap.containsKey(tuple.item.toString())) {
                    weekPartitionKey++;
                }

                // adding 1 week to the current tuple timestamp and marking the timestamp to consider as a change of the week.
                nextChunkTimestamp = getNextChunkTimestamp(tuple.time);

                // handling year change
                if (prevTupleDate.get(Calendar.YEAR) != getDate(tuple.time).get(Calendar.YEAR)) {
                    // updating the partition key of the year folder if it was created without the partition key.
                    if (!writingToExistingYearFolder) {
                        renameFolder(dataFile, yearPartitionKey, itemIdCounter, prevTupleDate.get(Calendar.YEAR));
                    }
                    yearPartitionKey = itemIdCounter;
                    writingToExistingYearFolder = false;
                }

                // create new week folder in case of year change.
                if (!writingToExistingYearFolder) {
                    dataFile = new Path(archiveLocation + getDate(tuple.time).get(Calendar.YEAR), "temp/data.parquet");
                } else {
                    // create new week folder under existing year folder for the same year.
                    int year = getDate(tuple.time).get(Calendar.YEAR);
                    File existingYearFolder = getExistingYearFolder(year);
                    dataFile = new Path(existingYearFolder.getAbsolutePath(), "temp/data.parquet");
                }
                parquetDataWriter.close();
                parquetDataWriter = getParquetDataWriter(dataFile);
            }

            // writing mappings and values
            internalTuple.setId(generateId(tuple, itemMappingWriter, propertyMappingWriter, contextMappingWriter));
            internalTuple.setTime(tuple.time);
            internalTuple.setSeqNr(tuple.seqNr);

            internalTuple.setValueInt(tuple.value instanceof Integer ? (int) tuple.value : null);
            internalTuple.setValueLong(tuple.value instanceof Long ? (long) tuple.value : null);
            internalTuple.setValueFloat(tuple.value instanceof Float ? (float) tuple.value : null);
            internalTuple.setValueDouble(tuple.value instanceof Double ? (double) tuple.value : null);
            internalTuple.setValueString(tuple.value instanceof String ? (String) tuple.value : null);
            internalTuple.setValueBool(tuple.value instanceof Boolean ? (Boolean) tuple.value ? 1 : 0 : null);
            if (tuple.value instanceof Record || tuple.value instanceof URI || tuple.value instanceof BigInteger || tuple.value instanceof BigDecimal || tuple.value instanceof Short) {
                internalTuple.setValueObject(encodeRecord(tuple.value));
            } else {
                internalTuple.setValueObject(null);
            }
            parquetDataWriter.write(internalTuple);
            prevTupleDate = getDate(tuple.time);
        }
        // updating last written week folder's partition key - for including last "WeekMaxItemPropertyContextId" for the week.
        renameFolder(dataFile, weekPartitionKey, itemIdCounter);
        // updating last written year folder's partition key - for including last "YearMaxItemPropertyContextId".
        renameFolder(dataFile, yearPartitionKey, itemIdCounter, prevTupleDate.get(Calendar.YEAR));
        itemMappingWriter.close();
        propertyMappingWriter.close();
        contextMappingWriter.close();
        parquetDataWriter.close();
    }

    private ParquetWriter<KvinTupleInternal> getParquetDataWriter(Path dataFile) throws IOException {
        Configuration writerConf = new Configuration();
        writerConf.setInt("parquet.zstd.compressionLevel", ZSTD_COMPRESSION_LEVEL);
        return AvroParquetWriter.<KvinTupleInternal>builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
                .withSchema(kvinTupleSchema)
                .withConf(writerConf)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                //.withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(PAGE_SIZE)
                .withDictionaryPageSize(DICT_PAGE_SIZE)
                .withDataModel(reflectData)
                .build();
    }

    private ParquetWriter<Object> getParquetMappingWriter(Path dataFile, Schema schema) throws IOException {
        Configuration writerConf = new Configuration();
        writerConf.setInt("parquet.zstd.compressionLevel", 12);
        return AvroParquetWriter.builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
                .withSchema(schema)
                .withConf(writerConf)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                //.withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(PAGE_SIZE)
                .withDictionaryPageSize(DICT_PAGE_SIZE)
                .withDataModel(reflectData)
                .build();
    }

    private Long initNextChunkTimestamp(Long nextChunkTimestamp, KvinTuple currentTuple) {
        // adding 1 week to the initial tuple timestamp and marking the timestamp to consider as a change of the week.
        if (nextChunkTimestamp == null) nextChunkTimestamp = getNextChunkTimestamp(currentTuple.time);
        return nextChunkTimestamp;
    }

    private long getNextChunkTimestamp(long currentTimestamp) {
        // adds 1 week to the given timestamp
        return currentTimestamp + 604800;
    }

    private void renameFolder(Path file, long newMin, long newMax) throws IOException {
        java.nio.file.Path currentFolder = Paths.get(file.getParent().toString());
        Files.move(currentFolder, currentFolder.resolveSibling(newMin + "_" + newMax));
    }

    private void renameFolder(Path file, long min, long max, int year) throws IOException {
        java.nio.file.Path currentFolder = Paths.get(file.getParent().getParent().toString());
        Files.move(currentFolder, currentFolder.resolveSibling(min + "_" + max + "_" + year));
    }

    private ArrayList<Integer> getExistingYears() {
        ArrayList<Integer> existingYears = new ArrayList<>();
        File[] yearFolders = new File(archiveLocation).listFiles();
        if (yearFolders != null) {
            for (File yearFolder : yearFolders) {
                String yearFolderName = yearFolder.getName();
                if (!yearFolderName.startsWith("metadata")) {
                    int year = Integer.parseInt(yearFolderName.split("_")[2]);
                    if (!existingYears.contains(year)) existingYears.add(year);
                }
            }
        }
        return existingYears;
    }

    private File getExistingYearFolder(int existingYear) {
        File[] yearFolders = new File(archiveLocation).listFiles();
        File existingYearFolder = null;
        for (File yearFolder : yearFolders) {
            if (!yearFolder.getName().startsWith("metadata")) {
                int year = Integer.parseInt(yearFolder.getName().split("_")[2]);
                if (year == existingYear) {
                    existingYearFolder = yearFolder;
                    break;
                }
            }
        }
        return existingYearFolder;
    }

    private Calendar getDate(long timestamp) {
        Timestamp ts = new Timestamp(timestamp * 1000);
        Date date = new java.sql.Date(ts.getTime());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar;
    }

    private byte[] generateId(KvinTuple currentTuple, ParquetWriter itemMappingWriter, ParquetWriter propertyMappingWriter, ParquetWriter contextMappingWriter) {
        long itemId = itemMap.computeIfAbsent(currentTuple.item.toString(), key -> {
            long newId = ++itemIdCounter;
            IdMapping mapping = new SimpleMapping();
            mapping.setId(newId);
            mapping.setValue(key);
            try {
                itemMappingWriter.write(mapping);
            } catch (IOException e) {
                throw new RuntimeException();
            }
            return newId;
        });
        long propertyId = propertyMap.computeIfAbsent(currentTuple.property.toString(), key -> {
            long newId = ++propertyIdCounter;
            IdMapping mapping = new SimpleMapping();
            mapping.setId(newId);
            mapping.setValue(key);
            try {
                propertyMappingWriter.write(mapping);
            } catch (IOException e) {
                throw new RuntimeException();
            }
            return newId;
        });

        long contextId = contextMap.computeIfAbsent(currentTuple.context.toString(), key -> {
            long newId = ++contextIdCounter;
            IdMapping mapping = new SimpleMapping();
            mapping.setId(newId);
            mapping.setValue(key);
            try {
                contextMappingWriter.write(mapping);
            } catch (IOException e) {
                throw new RuntimeException();
            }
            return newId;
        });

        ByteBuffer idBuffer = ByteBuffer.allocate(Long.BYTES * 3);
        idBuffer.putLong(itemId);
        idBuffer.putLong(propertyId);
        idBuffer.putLong(contextId);
        return idBuffer.array();
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

    private long getId(URI entity, IdType idType) {
        Cache<URI, Long> idCache;
        switch (idType) {
            case ITEM_ID:
                idCache = itemIdCache;
                break;
            case PROPERTY_ID:
                idCache = propertyIdCache;
                break;
            default:
            //case CONTEXT_ID:
                idCache = contextIdCache;
                break;
        }
        Long id = null;
        try {
            id = idCache.get(entity, () -> {
                // read from files
                String name;
                switch (idType) {
                    case ITEM_ID:
                        name ="item";
                        break;
                    case PROPERTY_ID:
                        name = "property";
                        break;
                    default:
                        //case CONTEXT_ID:
                        name = "context";
                        break;
                }
                FilterPredicate filter = eq(FilterApi.binaryColumn("value"), Binary.fromString(entity.toString()));
                Path mappingFile = new Path(this.archiveLocation + "metadata/" + name + "Mapping.parquet");
                IdMapping mapping = fetchMappingIds(mappingFile, filter);
                return mapping != null ? mapping.getId() : 0L;
            });
        } catch (ExecutionException e) {
            return 0L;
        }
        return id != null ? id : 0L;
    }

    private IdMappings getIdMappings(URI item, URI property, URI context) throws IOException {
        final IdMappings mappings = new IdMappings();
        if (item != null) {
            mappings.itemId = getId(item, IdType.ITEM_ID);
        }
        if (property != null) {
            mappings.propertyId = getId(property, IdType.PROPERTY_ID);
        }
        if (context != null) {
            mappings.contextId = getId(context, IdType.CONTEXT_ID);
        }
        return mappings;
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

    private FilterPredicate generateFetchFilter(IdMappings idMappings) {
        if (idMappings.propertyId != 0L) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES * 3);
            keyBuffer.putLong(idMappings.itemId);
            keyBuffer.putLong(idMappings.propertyId);
            keyBuffer.putLong(idMappings.contextId);
            return eq(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(keyBuffer.array()));
        } else {
            ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES);
            keyBuffer.putLong(idMappings.itemId);
            return and(gt(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(keyBuffer.array())),
                    lt(FilterApi.binaryColumn("id"),
                            Binary.fromConstantByteArray(ByteBuffer.allocate(Long.BYTES)
                                    .putLong(idMappings.itemId + 1).array())));
        }
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
        return fetchInternal(item, property, context, null, null, limit);
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
        IExtendedIterator<KvinTuple> internalResult = fetchInternal(item, property, context, end, begin, limit);
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

    public String getProperty(KvinTupleInternal tuple) {
        ByteBuffer idBuffer = ByteBuffer.wrap(tuple.getId());
        idBuffer.getLong();
        Long propertyId = idBuffer.getLong();
        String cachedProperty = propertyIdReverseLookUpCache.getIfPresent(propertyId);

        if (cachedProperty == null) {
            try {
                FilterPredicate filter = eq(FilterApi.longColumn("id"), propertyId);
                Path mappingFile = new Path(archiveLocation + "metadata/propertyMapping.parquet");
                IdMapping propertyMapping = fetchMappingIds(mappingFile, filter);
                cachedProperty = propertyMapping.getValue();
                propertyIdReverseLookUpCache.put(propertyId, propertyMapping.getValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return cachedProperty;
    }

    private IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, Long end, Long begin, Long limit) {
        try {
            // filters
            IdMappings idMappings = getIdMappings(item, property, context);
            if (idMappings.itemId == 0L) {
                return NiceIterator.emptyIterator();
            }

            FilterPredicate filter = generateFetchFilter(idMappings);
            if (begin != null) {
                filter = and(filter, gtEq(FilterApi.longColumn("time"), begin));
            }
            if (end != null) {
                filter = and(filter, lt(FilterApi.longColumn("time"), end));
            }

            final FilterPredicate filterFinal = filter;
            List<Path> dataFiles = getFilePath(idMappings);
            return new NiceIterator<KvinTuple>() {
                KvinTupleInternal internalTuple;
                ParquetReader<KvinTupleInternal> reader;
                long propertyValueCount;
                int fileIndex = -1;
                String currentProperty;

                {
                    try {
                        nextReader();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public boolean hasNext() {
                    try {
                        // skipping properties if limit is reached
                        if (limit != 0 && propertyValueCount >= limit) {
                            while ((internalTuple = reader.read()) != null) {
                                String property = getProperty(internalTuple);
                                if (!property.equals(currentProperty)) {
                                    propertyValueCount = 0;
                                    currentProperty = property;
                                    break;
                                }
                            }
                        }
                        internalTuple = reader.read();

                        if (internalTuple == null && fileIndex >= dataFiles.size() - 1) { // terminating condition
                            closeCurrentReader();
                            return false;
                        } else if (internalTuple == null && fileIndex < dataFiles.size() - 1 && propertyValueCount >= limit && limit != 0) { // moving on to the next reader upon limit reach
                            closeCurrentReader();
                            nextReader();
                            return hasNext();
                        } else if (internalTuple == null && fileIndex < dataFiles.size() - 1) { // moving on to the next available reader
                            closeCurrentReader();
                            nextReader();
                            internalTuple = reader.read();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return true;
                }

                @Override
                public KvinTuple next() {
                    if (internalTuple == null) {
                        throw new NoSuchElementException();
                    } else {
                        KvinTupleInternal tuple = internalTuple;
                        internalTuple = null;
                        return internalTupleToKvinTuple(tuple);
                    }
                }

                private KvinTuple internalTupleToKvinTuple(KvinTupleInternal internalTuple) {
                    Object value = null;
                    if (internalTuple.valueInt != null) {
                        value = internalTuple.valueInt;
                    } else if (internalTuple.valueLong != null) {
                        value = internalTuple.valueLong;
                    } else if (internalTuple.valueFloat != null) {
                        value = internalTuple.valueFloat;
                    } else if (internalTuple.valueDouble != null) {
                        value = internalTuple.valueDouble;
                    } else if (internalTuple.valueString != null) {
                        value = internalTuple.valueString;
                    } else if (internalTuple.valueBool != null) {
                        value = internalTuple.valueBool == 1;
                    } else if (internalTuple.valueObject != null) {
                        value = decodeRecord(internalTuple.valueObject);
                    }

                    // checking for property change
                    String property = getProperty(internalTuple);
                    if (currentProperty == null) {
                        currentProperty = property;
                    } else if (!property.equals(currentProperty)) {
                        currentProperty = property;
                        propertyValueCount = 0;
                    }

                    propertyValueCount++;
                    return new KvinTuple(item, URIs.createURI(property), context,
                            internalTuple.time, internalTuple.seqNr, value);
                }

                @Override
                public void close() {
                   closeCurrentReader();
                }

                void nextReader() throws IOException {
                    fileIndex++;
                    HadoopInputFile inputFile = getFile(dataFiles.get(fileIndex));
                    reader = AvroParquetReader.<KvinTupleInternal>builder(inputFile)
                            .withDataModel(reflectData)
                            .useStatsFilter()
                            .withFilter(FilterCompat.get(filterFinal))
                            .build();
                }

                void closeCurrentReader() {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                        }
                        reader = null;
                    }
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long delete(URI item, URI property, URI context, long end, long begin) {
        return 0;
    }

    @Override
    public boolean delete(URI item) {
        return false;
    }

    private ArrayList<Path> getFilePath(IdMappings idMappings) {
        File archiveFolder = new File(archiveLocation);
        File[] yearWiseFolders = archiveFolder.listFiles();
        ArrayList<Path> matchedFiles = new ArrayList<>();

        long itemId = idMappings.itemId;

        // matching ids with relevant parquet files
        if (yearWiseFolders != null) {
            for (File yearFolder : yearWiseFolders) {
                try {
                    String[] folderIdMinMaxData = yearFolder.getName().split("_");
                    if (folderIdMinMaxData[0].contains("metadata")) {
                        continue;
                    }
                    long yearMinPartitionKey = Long.parseLong(folderIdMinMaxData[0]);
                    long yearMaxPartitionKey = Long.parseLong(folderIdMinMaxData[1]);

                    if (itemId >= yearMinPartitionKey && itemId <= yearMaxPartitionKey) {
                        for (File weekFolder : new File(yearFolder.getPath()).listFiles()) {
                            try {
                                String[] weekFolderIdMinMaxData = weekFolder.getName().split("_");
                                long weekMinPartitionKey = Long.parseLong(weekFolderIdMinMaxData[0]);
                                long weekMaxPartitionKey = Long.parseLong(weekFolderIdMinMaxData[1]);
                                if (itemId >= weekMinPartitionKey && itemId <= weekMaxPartitionKey) {
                                    Path path = new Path(weekFolder.getPath() + "/data.parquet");
                                    if (!matchedFiles.contains(path)) matchedFiles.add(path);
                                    break;
                                }
                            } catch (RuntimeException ignored) {
                            }
                        }
                    }
                } catch (RuntimeException ignored) {
                }
            }
        }
        return matchedFiles;
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
            IdMappings idMappings = getIdMappings(item, null, Kvin.DEFAULT_CONTEXT);
            if (idMappings.itemId == 0L) {
                return NiceIterator.emptyIterator();
            }
            FilterPredicate filter = generateFetchFilter(idMappings);
            ArrayList<Path> dataFiles = getFilePath(idMappings);
            ArrayList<ParquetReader<KvinTupleInternal>> readers = new ArrayList<>();

            // data readers
            for (Path path : dataFiles) {
                readers.add(AvroParquetReader.<KvinTupleInternal>builder(HadoopInputFile.fromPath(path, new Configuration()))
                        .withDataModel(reflectData)
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
                    return URIs.createURI("");
                    //return URIs.createURI(internalTuple.getProperty());
                }

                @Override
                public void close() {
                    try {
                        for (ParquetReader<?> reader : readers) {
                            reader.close();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
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

    // id enum
    enum IdType {
        ITEM_ID,
        PROPERTY_ID,
        CONTEXT_ID
    }

    interface IdMapping {
        long getId();

        void setId(long id);

        String getValue();

        void setValue(String value);
    }

    public static class KvinTupleInternal {
        private byte[] id;
        private Long time;
        private Integer seqNr;
        private Integer valueInt;
        private Long valueLong;
        private Float valueFloat;
        private Double valueDouble;
        private String valueString;
        private Integer valueBool;
        private byte[] valueObject;

        private String archiveLocation;

        public byte[] getId() {
            return id;
        }

        public void setId(byte[] id) {
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

        public Integer getValueInt() {
            return valueInt;
        }

        public void setValueInt(Integer valueInt) {
            this.valueInt = valueInt;
        }

        public Long getValueLong() {
            return valueLong;
        }

        public void setValueLong(Long valueLong) {
            this.valueLong = valueLong;
        }

        public Float getValueFloat() {
            return valueFloat;
        }

        public void setValueFloat(Float valueFloat) {
            this.valueFloat = valueFloat;
        }

        public Double getValueDouble() {
            return valueDouble;
        }

        public void setValueDouble(Double valueDouble) {
            this.valueDouble = valueDouble;
        }

        public String getValueString() {
            return valueString;
        }

        public void setValueString(String valueString) {
            this.valueString = valueString;
        }

        public byte[] getValueObject() {
            return valueObject;
        }

        public void setValueObject(byte[] valueObject) {
            this.valueObject = valueObject;
        }

        public Integer getValueBool() {
            return valueBool;
        }

        public void setValueBool(Integer valueBool) {
            this.valueBool = valueBool;
        }
    }

    public static class SimpleMapping implements IdMapping {
        long id;
        String value;

        @Override
        public long getId() {
            return id;
        }

        @Override
        public void setId(long id) {
            this.id = id;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public void setValue(String value) {
            this.value = value;
        }
    }
}
