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
import java.util.*;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class KvinParquet implements Kvin {
    final int ROW_GROUP_SIZE = 10485760;  // 10 MB
    final int PAGE_SIZE = 6144; // 6 KB
    final int DICT_PAGE_SIZE = 5242880; // 5 MB
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
        Path dataFile = new Path("./target/archive", "temp.parquet");
        ParquetWriter<KvinTupleInternal> parquetDataWriter = AvroParquetWriter.<KvinTupleInternal>builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
                .withSchema(kvinTupleSchema)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(PAGE_SIZE)
                .withDictionaryPageSize(DICT_PAGE_SIZE)
                .withDataModel(ReflectData.get())
                .build();

        // mapping writer
        Path mappingFile = new Path("./target/archive", "data.mapping.parquet");
        ParquetWriter<Mapping> parquetMappingWriter = AvroParquetWriter.<Mapping>builder(HadoopOutputFile.fromPath(mappingFile, new Configuration()))
                .withSchema(mappingSchema)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(PAGE_SIZE)
                .withDictionaryPageSize(DICT_PAGE_SIZE)
                .withDataModel(ReflectData.get())
                .build();

        Long nextChunkTimestamp = null;
        int min = 1;

        for (KvinTuple tuple : tuples) {
            KvinTupleInternal internalTuple = new KvinTupleInternal();

            if (nextChunkTimestamp == null) nextChunkTimestamp = getNextChunkTimestamp(tuple.time);

            if (tuple.time >= nextChunkTimestamp) {
                //renaming existing file with max id
                renameFile(dataFile, min, idCounter);
                parquetDataWriter.close();

                min = idCounter + 1;
                nextChunkTimestamp = getNextChunkTimestamp(tuple.time);

                dataFile = new Path("./target/archive", "temp.parquet");
                parquetDataWriter = AvroParquetWriter.<KvinTupleInternal>builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
                        .withSchema(kvinTupleSchema)
                        .withDictionaryEncoding(true)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withRowGroupSize(ROW_GROUP_SIZE)
                        .withPageSize(PAGE_SIZE)
                        .withDictionaryPageSize(DICT_PAGE_SIZE)
                        .withDataModel(ReflectData.get())
                        .build();
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
        }
        renameFile(dataFile, min, idCounter);
        parquetMappingWriter.close();
        parquetDataWriter.close();
    }

    private long getNextChunkTimestamp(long currentTimestamp) {
        return currentTimestamp + 604800;
    }

    private void renameFile(Path file, int min, int max) throws IOException {
        java.nio.file.Path currentFile = Paths.get(file.toString());
        Files.move(currentFile, currentFile.resolveSibling(min + "_" + max + "_" + ".parquet"));
    }

    private int generateId() {
        return ++idCounter;
    }

    private ArrayList<Mapping> getIdMapping(URI item, URI property, URI context) throws IOException {
        ArrayList<Mapping> mappings = new ArrayList<>();
        Path file = new Path("./target/archive/data.mapping.parquet");
        FilterPredicate filter = null;
        if (item != null && property != null) {
            filter = and(eq(FilterApi.binaryColumn("item"), Binary.fromString(item.toString())), eq(FilterApi.binaryColumn("property"), Binary.fromString(property.toString())));
        } else if (item != null) {
            filter = eq(FilterApi.binaryColumn("item"), Binary.fromString(item.toString()));
        }
        try (ParquetReader<Mapping> reader = AvroParquetReader.<Mapping>builder(HadoopInputFile.fromPath(file, new Configuration()))
                .withDataModel(new ReflectData(Mapping.class.getClassLoader()))
                .withFilter(FilterCompat.get(filter))
                .build()) {

            Mapping mapping;
            while ((mapping = reader.read()) != null) {
                mappings.add(mapping);
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
            Path dataFile = new Path(getFilePath(idMappings));
            // data reader
            try (ParquetReader<KvinTupleInternal> reader = AvroParquetReader.<KvinTupleInternal>builder(HadoopInputFile.fromPath(dataFile, new Configuration()))
                    .withDataModel(new ReflectData(KvinTupleInternal.class.getClassLoader()))
                    .withFilter(FilterCompat.get(filter))
                    .build()) {

                return new NiceIterator<>() {
                    KvinTupleInternal internalTuple;
                    HashMap<String, Integer> itemPropertyCount = new HashMap<>();
                    int propertyCount = 0;
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

                        return internalTuple != null;
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
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getFilePath(ArrayList<Mapping> idMappings) {
        int id = idMappings.get(0).getId();
        File folder = new File("./target/archive/");
        File[] dataFiles = folder.listFiles();
        File matchedFile = null;

        if (dataFiles != null) {
            for (File file : dataFiles) {
                try {
                    String[] idMinMaxData = file.getName().split("_");
                    int min = Integer.valueOf(idMinMaxData[0]);
                    int max = Integer.valueOf(idMinMaxData[1]);
                    if (id >= min && id <= max) {
                        matchedFile = file;
                        break;
                    }
                } catch (RuntimeException exception) {
                }
            }
        }
        return matchedFile.getPath();
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
        Path file = new Path("./target/test.data.parquet");

        try {
            // filters
            ArrayList<Mapping> idMappings = getIdMapping(item, null, null);
            FilterPredicate filter = generateFilterPredicates(idMappings, 0);

            // data reader
            try (ParquetReader<KvinTupleInternal> reader = AvroParquetReader.<KvinTupleInternal>builder(HadoopInputFile.fromPath(file, new Configuration()))
                    .withDataModel(new ReflectData(KvinTupleInternal.class.getClassLoader()))
                    .withFilter(FilterCompat.get(filter))
                    .build()) {

                return new NiceIterator<>() {
                    KvinTupleInternal internalTuple;
                    int propertyCount = 0;

                    @Override
                    public boolean hasNext() {
                        try {
                            internalTuple = reader.read();
                            return internalTuple != null;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
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
            }
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
