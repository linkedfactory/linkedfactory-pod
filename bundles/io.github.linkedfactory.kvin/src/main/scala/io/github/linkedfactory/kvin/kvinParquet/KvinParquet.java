package io.github.linkedfactory.kvin.kvinParquet;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinListener;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
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
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

import static org.apache.parquet.filter2.predicate.FilterApi.eq;

public class KvinParquet implements Kvin {
    int idCounter = 0;

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
        Path dataFile = new Path("./target/test.data.parquet");
        Path mappingFile = new Path("./target/test.mapping.parquet");

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
                .name("value").type().nullable().bytesType().noDefault().endRecord();

        // mapping writer
        ParquetWriter<Mapping> parquetMappingWriter = AvroParquetWriter.<Mapping>builder(mappingFile)
                .withSchema(mappingSchema)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(33554432) // 32 MB
                .withPageSize(1048576) // 1 MB
                .withDictionaryPageSize(2097152) // 2 MB
                .withDataModel(ReflectData.get())
                .build();
        // data writer
        ParquetWriter<KvinTupleInternal> parquetDataWriter = AvroParquetWriter.<KvinTupleInternal>builder(dataFile)
                .withSchema(kvinTupleSchema)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(33554432) // 32 MB
                .withPageSize(1048576) // 1 MB
                .withDictionaryPageSize(2097152) // 2 MB
                .withDataModel(ReflectData.get())
                .build();

        Iterator<KvinTuple> iterator = tuples.iterator();
        while (iterator.hasNext()) {
            KvinTuple tuple = iterator.next();
            KvinTupleInternal internalTuple = new KvinTupleInternal();
            internalTuple.setId(generateId(tuple.item, tuple.property, tuple.context, parquetMappingWriter));
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
            parquetDataWriter.write(internalTuple);
        }
        parquetMappingWriter.close();
        parquetDataWriter.close();
    }

    private int generateId(URI item, URI property, URI context, ParquetWriter<Mapping> mappingParquetWriter) throws IOException {
        Mapping mapping = new Mapping();
        mapping.setId(++idCounter);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write((item.toString() + "^^" + property.toString() + "^^" + context.toString()).getBytes());
        mapping.setValue(byteArrayOutputStream.toByteArray());
        mappingParquetWriter.write(mapping);
        return idCounter;
    }

    private Mapping getIdMapping(String item, String property, String context) throws IOException {
        Path file = new Path("./target/test.mapping.parquet");
        FilterPredicate filter = eq(FilterApi.binaryColumn("value"), Binary.fromString((item + "^^" + property + "^^" + context)));
        ParquetReader<Mapping> reader = AvroParquetReader.<Mapping>builder(HadoopInputFile.fromPath(file, new Configuration()))
                .withDataModel(new ReflectData(Mapping.class.getClassLoader()))
                .withFilter(FilterCompat.get(filter))
                .build();
        return reader.read();
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
        return fetchInternal(item, property, context, end, begin, limit, interval, op);
    }

    private IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, Long end, Long begin, Long limit, Long interval, String op) {
        Path file = new Path("./target/test.data.parquet");

        try {
            // filters
            Mapping idMapping = getIdMapping(item.toString(), property.toString(), context.toString());
            FilterPredicate filter = eq(FilterApi.intColumn("id"), idMapping.getId() != null ? idMapping.getId() : -1);
            // data reader
            ParquetReader<KvinTupleInternal> reader = AvroParquetReader.<KvinTupleInternal>builder(HadoopInputFile.fromPath(file, new Configuration()))
                    .withDataModel(new ReflectData(KvinTupleInternal.class.getClassLoader()))
                    .withFilter(FilterCompat.get(filter))
                    .build();
            return new NiceIterator<>() {
                KvinTupleInternal internalTuple;

                @Override
                public boolean hasNext() {
                    try {
                        internalTuple = reader.read();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return internalTuple != null ? true : false;
                }

                @Override
                public KvinTuple next() {
                    if (internalTuple != null) {
                        return internalTupleToKvinTuple(internalTuple);
                    } else {
                        return null;
                    }
                }

                @Override
                public void close() {
                    super.close();
                }

                private KvinTuple internalTupleToKvinTuple(KvinTupleInternal internalTuple) {
                    String[] idValues = new String(idMapping.getValue(), StandardCharsets.UTF_8).split("\\^\\^");
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
                    return new KvinTuple(URIs.createURI(idValues[0]), URIs.createURI(idValues[1]), URIs.createURI(idValues[2]), internalTuple.time, internalTuple.seqNr, value);

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
        return null;
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
        byte[] value;


        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }
    }
}
