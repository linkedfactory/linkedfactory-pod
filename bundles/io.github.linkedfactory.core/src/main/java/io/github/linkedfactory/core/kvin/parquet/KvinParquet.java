package io.github.linkedfactory.core.kvin.parquet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.util.AggregatingIterator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.util.Pair;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.ReadPrefReadWriteLockManager;
import org.eclipse.rdf4j.common.concurrent.locks.ReadWriteLockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.linkedfactory.core.kvin.parquet.ParquetHelpers.*;
import static io.github.linkedfactory.core.kvin.parquet.Records.decodeRecord;
import static io.github.linkedfactory.core.kvin.parquet.Records.encodeRecord;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class KvinParquet implements Kvin {
	static final Logger log = LoggerFactory.getLogger(KvinParquet.class);

	// used by reader
	final Cache<URI, Long> itemIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	final Cache<URI, Long> propertyIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	final Cache<URI, Long> contextIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	// Lock
	Map<Path, HadoopInputFile> inputFileCache = new HashMap<>(); // hadoop input file cache
	Cache<Long, URI> propertyIdReverseLookUpCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	Cache<java.nio.file.Path, Properties> metaCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	String archiveLocation;

	ReadWriteLockManager lockManager = new ReadPrefReadWriteLockManager(true, 5000);

	public KvinParquet(String archiveLocation) {
		this.archiveLocation = archiveLocation;
		if (!this.archiveLocation.endsWith("/")) {
			this.archiveLocation = this.archiveLocation + "/";
		}
	}

	private IdMapping fetchMappingIds(Path mappingFile, FilterPredicate filter) throws IOException {
		IdMapping id;
		HadoopInputFile inputFile = getFile(mappingFile);
		try (ParquetReader<IdMapping> reader = createReader(inputFile, FilterCompat.get(filter))) {
			id = reader.read();
		}
		return id;
	}

	private void readMaxIds(WriteContext writeContext, java.nio.file.Path metadataPath) throws IOException {
		Map<String, List<Pair<String, Integer>>> mappingFiles = getMappingFiles(metadataPath);
		ColumnPath idPath = ColumnPath.get("id");
		for (Map.Entry<String, List<Pair<String, Integer>>> entry : mappingFiles.entrySet()) {
			long maxId = 0L;
			for (Pair<String, Integer> mappingFile : entry.getValue()) {
				HadoopInputFile inputFile = getFile(new Path(metadataPath.resolve(mappingFile.getFirst()).toString()));
				ParquetReadOptions readOptions = HadoopReadOptions
						.builder(inputFile.getConfiguration(), inputFile.getPath())
						.build();

				ParquetMetadata meta = ParquetFileReader.readFooter(inputFile, readOptions, inputFile.newStream());
				for (BlockMetaData blockMeta : meta.getBlocks()) {
					for (ColumnChunkMetaData columnMeta : blockMeta.getColumns()) {
						if (columnMeta.getPath().equals(idPath)) {
							// get max id from statistics
							maxId = Math.max(maxId, ((Number) columnMeta.getStatistics().genericGetMax()).longValue());
						}
					}
				}
			}
			switch (entry.getKey()) {
				case "items":
					writeContext.itemIdCounter = maxId;
					break;
				case "properties":
					writeContext.propertyIdCounter = maxId;
					break;
				case "contexts":
					writeContext.contextIdCounter = maxId;
					break;
			}
		}
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

	Lock writeLock() throws IOException {
		try {
			return lockManager.getWriteLock();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	Lock readLock() throws IOException {
		try {
			return lockManager.getReadLock();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
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

	private synchronized void putInternal(Iterable<KvinTuple> tuples) throws IOException {
		ClassLoader contextCl = Thread.currentThread().getContextClassLoader();
		Lock writeLock = null;
		try {
			Thread.currentThread().setContextClassLoader(KvinParquet.class.getClassLoader());

			writeLock = writeLock();

			java.nio.file.Path metadataPath = Paths.get(archiveLocation, "metadata");
			WriteContext writeContext = new WriteContext();
			writeContext.hasExistingData = Files.exists(metadataPath);
			if (writeContext.hasExistingData) {
				readMaxIds(writeContext, metadataPath);
			}

			Map<String, WriterState> writers = new HashMap<>();

			java.nio.file.Path tempPath = Paths.get(archiveLocation, ".tmp");
			if (Files.exists(tempPath)) {
				// delete temporary files if a previous put was not finished completely
				Files.walk(tempPath).sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile)
						.forEach(File::delete);
			}
			writeLock.release();

			Files.createDirectories(tempPath);
			Path itemMappingFile = new Path(tempPath.toString(), "metadata/items__1.parquet");
			Path propertyMappingFile = new Path(tempPath.toString(), "metadata/properties__1.parquet");
			Path contextMappingFile = new Path(tempPath.toString(), "metadata/contexts__1.parquet");

			ParquetWriter<Object> itemMappingWriter = getParquetMappingWriter(itemMappingFile);
			ParquetWriter<Object> propertyMappingWriter = getParquetMappingWriter(propertyMappingFile);
			ParquetWriter<Object> contextMappingWriter = getParquetMappingWriter(contextMappingFile);

			WriterState writerState = null;
			String prevKey = null;
			for (KvinTuple tuple : tuples) {
				KvinTupleInternal internalTuple = new KvinTupleInternal();

				Calendar tupleDate = getDate(tuple.time);
				int year = tupleDate.get(Calendar.YEAR);
				int week = tupleDate.get(Calendar.WEEK_OF_YEAR);

				String key = year + "_" + week;
				if (!key.equals(prevKey)) {
					writerState = writers.get(key);
					if (writerState == null) {
						java.nio.file.Path file = tempPath.resolve(key + "_data.parquet");
						writerState = new WriterState(file, getParquetDataWriter(new Path(file.toString())),
								year, week);
						writers.put(key, writerState);
					}
					prevKey = key;
				}

				// writing mappings and values
				internalTuple.setId(generateId(tuple, writeContext,
						itemMappingWriter, propertyMappingWriter, contextMappingWriter));
				internalTuple.setTime(tuple.time);
				internalTuple.setSeqNr(tuple.seqNr);

				internalTuple.setValueInt(tuple.value instanceof Integer ? (int) tuple.value : null);
				internalTuple.setValueLong(tuple.value instanceof Long ? (long) tuple.value : null);
				internalTuple.setValueFloat(tuple.value instanceof Float ? (float) tuple.value : null);
				internalTuple.setValueDouble(tuple.value instanceof Double ? (double) tuple.value : null);
				internalTuple.setValueString(tuple.value instanceof String ? (String) tuple.value : null);
				internalTuple.setValueBool(tuple.value instanceof Boolean ? (Boolean) tuple.value ? 1 : 0 : null);
				if (tuple.value instanceof Record || tuple.value instanceof URI || tuple.value instanceof BigInteger ||
						tuple.value instanceof BigDecimal || tuple.value instanceof Short || tuple.value instanceof Object[]) {
					internalTuple.setValueObject(encodeRecord(tuple.value));
				} else {
					internalTuple.setValueObject(null);
				}
				writerState.writer.write(internalTuple);
				writerState.minMax[0] = Math.min(writerState.minMax[0], writeContext.itemIdCounter);
				writerState.minMax[1] = Math.max(writerState.minMax[1], writeContext.itemIdCounter);
			}

			for (WriterState state : writers.values()) {
				state.writer.close();
			}

			boolean itemsWritten = itemMappingWriter.getDataSize() > 0;
			itemMappingWriter.close();
			boolean contextsWritten = contextMappingWriter.getDataSize() > 0;
			contextMappingWriter.close();
			boolean propertiesWritten = propertyMappingWriter.getDataSize() > 0;
			propertyMappingWriter.close();

			if (!itemsWritten) {
				Files.delete(Paths.get(itemMappingFile.toString()));
			}
			if (!contextsWritten) {
				Files.delete(Paths.get(contextMappingFile.toString()));
			}
			if (!propertiesWritten) {
				Files.delete(Paths.get(propertyMappingFile.toString()));
			}

			Map<Integer, long[]> minMaxYears = new HashMap<>();
			for (WriterState state : writers.values()) {
				minMaxYears.compute(state.year, (k, v) -> {
					if (v == null) {
						return Arrays.copyOf(state.minMax, state.minMax.length);
					} else {
						v[0] = Math.min(v[0], state.minMax[0]);
						v[1] = Math.max(v[1], state.minMax[1]);
						return v;
					}
				});
			}

			java.nio.file.Path metaPath = Paths.get(archiveLocation, "meta.properties");
			Properties meta = new Properties();
			if (Files.exists(metaPath)) {
				meta.load(Files.newInputStream(metaPath));
			}
			meta.stringPropertyNames().forEach(yearStr -> {
				String idRange = String.valueOf(meta.get(yearStr));
				minMaxYears.computeIfPresent(Integer.parseInt(yearStr), (k, v) -> {
					long[] minMaxYear = splitRange(idRange);
					v[0] = Math.min(v[0], minMaxYear[0]);
					v[1] = Math.max(v[1], minMaxYear[1]);
					return v;
				});
			});
			Files.walk(Paths.get(archiveLocation), 1).skip(1).forEach(parent -> {
				if (!tempPath.equals(parent) && Files.isDirectory(parent)) {
					java.nio.file.Path yearMetaPath = parent.resolve("meta.properties");
					if (Files.exists(yearMetaPath)) {
						Properties yearMeta = new Properties();
						try {
							yearMeta.load(Files.newInputStream(yearMetaPath));
						} catch (IOException e) {
							log.error("Error while loading meta data", e);
						}
						yearMeta.stringPropertyNames().forEach(week -> {
							String idRange = String.valueOf(yearMeta.get(week));
							String key = parent.getFileName() + "_" + week;

							WriterState state = writers.get(key);
							if (state != null) {
								long[] minMaxWeek = splitRange(idRange);
								if (minMaxWeek != null) {
									state.minMax[0] = Math.min(state.minMax[0], minMaxWeek[0]);
									state.minMax[1] = Math.max(state.minMax[1], minMaxWeek[1]);
								}
							}
						});
					}
				}
			});

			writeLock = writeLock();

			Map<Integer, List<WriterState>> writersPerYear = writers.values().stream()
					.collect(Collectors.groupingBy(s -> s.year));
			for (Map.Entry<Integer, List<WriterState>> entry : writersPerYear.entrySet()) {
				int year = entry.getKey();
				String yearFolderName = String.format("%04d", year);
				long[] minMaxYear = minMaxYears.get(year);
				meta.put(yearFolderName, minMaxYear[0] + "-" + minMaxYear[1]);
			}
			meta.store(Files.newOutputStream(metaPath), null);

			for (Map.Entry<Integer, List<WriterState>> entry : writersPerYear.entrySet()) {
				int year = entry.getKey();
				String yearFolderName = String.format("%04d", year);
				java.nio.file.Path yearFolder = Paths.get(archiveLocation, yearFolderName);
				Files.createDirectories(yearFolder);

				java.nio.file.Path yearMetaPath = yearFolder.resolve("meta.properties");
				Properties yearMeta = new Properties();
				if (Files.exists(yearMetaPath)) {
					yearMeta.load(Files.newInputStream(yearMetaPath));
				}
				for (WriterState state : entry.getValue()) {
					String weekFolderName = String.format("%02d", state.week);
					String idRange = yearMeta.getProperty(weekFolderName);
					if (idRange != null) {
						long[] minMax = splitRange(idRange);
						yearMeta.put(weekFolderName, Math.min(minMax[0], state.minMax[0]) + "-" + Math.max(minMax[1], state.minMax[1]));
					} else {
						yearMeta.put(weekFolderName, state.minMax[0] + "-" + state.minMax[1]);
					}

					java.nio.file.Path weekFolder = yearFolder.resolve(weekFolderName);
					Files.createDirectories(weekFolder);
					int maxSeqNr = Files.list(weekFolder).map(p -> {
						String name = p.getFileName().toString();
						if (name.startsWith("data")) {
							Matcher m = fileWithSeqNr.matcher(name);
							if (m.matches()) {
								return Integer.parseInt(m.group(2));
							}
						}
						return 0;
					}).max(Integer::compareTo).orElse(0);
					String filename = "data__" + (maxSeqNr + 1) + ".parquet";

					log.debug("moving: " + state.file + " -> " + weekFolder.resolve(filename));
					Files.move(state.file, weekFolder.resolve(filename));
				}
				yearMeta.store(Files.newOutputStream(yearMetaPath), null);
			}

			java.nio.file.Path tempMetadataPath = tempPath.resolve("metadata");
			Files.createDirectories(metadataPath);
			Map<String, List<Pair<String, Integer>>> newMappingFiles = getMappingFiles(tempMetadataPath);
			Map<String, List<Pair<String, Integer>>> existingMappingFiles = getMappingFiles(metadataPath);
			for (Map.Entry<String, List<Pair<String, Integer>>> newMapping : newMappingFiles.entrySet()) {
				int seqNr = Optional.ofNullable(existingMappingFiles.get(newMapping.getKey()))
						.filter(l -> !l.isEmpty())
						.map(l -> l.get(l.size() - 1).getSecond())
						.orElse(0) + 1;
				Files.move(tempMetadataPath.resolve(newMapping.getValue().get(0).getFirst()),
						metadataPath.resolve(newMapping.getKey() + "__" + seqNr + ".parquet"));
			}

			// completely delete temporary directory
			Files.walk(tempPath).sorted(Comparator.reverseOrder())
					.map(java.nio.file.Path::toFile)
					.forEach(File::delete);

			// clear cache with meta data
			metaCache.invalidateAll();

			// invalidate id caches - TODO could be improved by directly updating the caches
			itemIdCache.invalidateAll();
			propertyIdCache.invalidateAll();
			contextIdCache.invalidateAll();
		} catch (Throwable e) {
			log.error("Error while adding data", e);
		} finally {
			if (writeLock != null) {
				writeLock.release();
			}
			Thread.currentThread().setContextClassLoader(contextCl);
		}
	}

	private Calendar getDate(long timestamp) {
		Timestamp ts = new Timestamp(timestamp);
		Date date = new java.sql.Date(ts.getTime());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar;
	}

	private byte[] generateId(KvinTuple tuple,
	                          WriteContext writeContext,
	                          ParquetWriter itemMappingWriter,
	                          ParquetWriter propertyMappingWriter,
	                          ParquetWriter contextMappingWriter) {
		long itemId = writeContext.itemMap.computeIfAbsent(tuple.item.toString(), key -> {
			if (writeContext.hasExistingData) {
				long id = getId(tuple.item, IdType.ITEM_ID);
				if (id != 0L) {
					return id;
				}
			}
			long newId = ++writeContext.itemIdCounter;
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
		long propertyId = writeContext.propertyMap.computeIfAbsent(tuple.property.toString(), key -> {
			if (writeContext.hasExistingData) {
				long id = getId(tuple.property, IdType.PROPERTY_ID);
				if (id != 0L) {
					return id;
				}
			}
			long newId = ++writeContext.propertyIdCounter;
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

		long contextId = writeContext.contextMap.computeIfAbsent(tuple.context.toString(), key -> {
			if (writeContext.hasExistingData) {
				long id = getId(tuple.context, IdType.CONTEXT_ID);
				if (id != 0L) {
					return id;
				}
			}
			long newId = ++writeContext.contextIdCounter;
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
		Long id;
		try {
			id = idCache.get(entity, () -> {
				// read from files
				String name;
				switch (idType) {
					case ITEM_ID:
						name = "items";
						break;
					case PROPERTY_ID:
						name = "properties";
						break;
					default:
						//case CONTEXT_ID:
						name = "contexts";
						break;
				}
				FilterPredicate filter = eq(FilterApi.binaryColumn("value"), Binary.fromString(entity.toString()));
				File[] mappingFiles = new File(this.archiveLocation + "metadata/").listFiles((file, s) -> s.startsWith(name));
				if (mappingFiles == null) {
					return 0L;
				}
				IdMapping mapping = null;
				for (File mappingFile : mappingFiles) {
					mapping = fetchMappingIds(new Path(mappingFile.getPath()), filter);
					if (mapping != null) break;
				}
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

	private FilterPredicate generateFetchFilter(IdMappings idMappings) {
		if (idMappings.propertyId != 0L && idMappings.contextId != 0L) {
			ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES * 3);
			keyBuffer.putLong(idMappings.itemId);
			keyBuffer.putLong(idMappings.propertyId);
			keyBuffer.putLong(idMappings.contextId);
			return eq(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(keyBuffer.array()));
		} else if (idMappings.propertyId != 0L) {
			ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES * 2);
			keyBuffer.putLong(idMappings.itemId);
			keyBuffer.putLong(idMappings.propertyId);
			return and(gt(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(keyBuffer.array())),
					lt(FilterApi.binaryColumn("id"),
							Binary.fromConstantByteArray(ByteBuffer.allocate(Long.BYTES * 2)
									.putLong(idMappings.itemId).putLong(idMappings.propertyId + 1).array())));
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
		try {
			return fetchInternal(item, property, context, null, null, limit);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
		try {
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
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public URI getProperty(ByteBuffer idBuffer) throws IOException {
		idBuffer.getLong();
		Long propertyId = idBuffer.getLong();
		URI cachedProperty = propertyIdReverseLookUpCache.getIfPresent(propertyId);

		if (cachedProperty == null) {
			FilterPredicate filter = eq(FilterApi.longColumn("id"), propertyId);
			Path metadataFolder = new Path(this.archiveLocation + "metadata/");
			File[] mappingFiles = new File(metadataFolder.toString()).listFiles((file, s) -> s.startsWith("properties"));
			IdMapping propertyMapping = null;

			for (File mappingFile : mappingFiles) {
				propertyMapping = fetchMappingIds(new Path(mappingFile.getPath()), filter);
				if (propertyMapping != null) break;
			}

			if (propertyMapping == null) {
				throw new IOException("Unknown property with id: " + propertyId);
			} else {
				cachedProperty = URIs.createURI(propertyMapping.getValue());
			}
			propertyIdReverseLookUpCache.put(propertyId, cachedProperty);
		}
		return cachedProperty;
	}

	private <T> ParquetReader<T> createReader(InputFile file, FilterCompat.Filter filter) throws IOException {
		return AvroParquetReader.<T>builder(file)
				.withDataModel(reflectData)
				.useStatsFilter()
				.withFilter(filter)
				.build();
	}

	private ParquetReader<GenericRecord> createGenericReader(InputFile file, FilterCompat.Filter filter) throws IOException {
		return AvroParquetReader.<GenericRecord>builder(file)
				.withDataModel(GenericData.get())
				.useStatsFilter()
				.withFilter(filter)
				.build();
	}

	static Comparator<KvinTuple> TUPLE_COMPARATOR = (a, b) -> {
		int diff = a.property.equals(b.property) ? 0 : a.toString().compareTo(b.toString());
		if (diff != 0) {
			return diff;
		}
		diff = Long.compare(a.time, b.time);
		if (diff != 0) {
			// time is reverse
			return -diff;
		}
		diff = Integer.compare(a.seqNr, b.seqNr);
		if (diff != 0) {
			// seqNr is reverse
			return -diff;
		}
		return 0;
	};

	private synchronized IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, Long end, Long begin, Long limit) throws IOException {
		Lock readLock = readLock();
		try {
			// filters
			IdMappings idMappings = getIdMappings(item, property, context);
			if (idMappings.itemId == 0L
					|| property != null && idMappings.propertyId == 0L
					|| context != null && idMappings.contextId == 0L) {
				// ensure read lock is freed
				readLock.release();
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
			List<java.nio.file.Path> dataFolders = getDataFolders(idMappings);
			if (dataFolders.isEmpty()) {
				// ensure read lock is freed
				readLock.release();
				return NiceIterator.emptyIterator();
			}
			return new NiceIterator<KvinTuple>() {
				final PriorityQueue<Pair<KvinTuple, ParquetReader<GenericRecord>>> nextTuples =
						new PriorityQueue<>(Comparator.comparing(Pair::getFirst, TUPLE_COMPARATOR));
				KvinTuple prevTuple, nextTuple;
				long propertyValueCount;
				int folderIndex = -1;
				boolean closed;

				{
					try {
						nextReaders();
						if (property == null) {
							// directly load all relevant files if property is not given
							// as data might be distributed over multiple files for one property
							while (folderIndex < dataFolders.size() - 1) {
								nextReaders();
							}
						}
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				}

				KvinTuple selectNextTuple() throws IOException {
					while (true) {
						while (nextTuples.isEmpty() && folderIndex < dataFolders.size() - 1) {
							nextReaders();
						}
						var min = nextTuples.isEmpty() ? null : nextTuples.poll();
						if (min != null) {
							// omit duplicates in terms of id, time, and seqNr
							boolean isDuplicate = prevTuple != null && TUPLE_COMPARATOR.compare(prevTuple, min.getFirst()) == 0;

							var record = min.getSecond().read();
							if (record != null) {
								KvinTuple tuple = convert(record);
								nextTuples.add(new Pair<>(tuple, min.getSecond()));
							} else {
								try {
									min.getSecond().close();
								} catch (IOException e) {
								}
							}
							if (!isDuplicate) {
								return min.getFirst();
							}
						} else {
							break;
						}
					}
					return null;
				}

				@Override
				public boolean hasNext() {
					if (nextTuple != null) {
						return true;
					}
					try {
						// skipping properties if limit is reached
						if (limit != 0 && propertyValueCount >= limit) {
							while ((nextTuple = selectNextTuple()) != null) {
								if (!nextTuple.property.equals(prevTuple.property)) {
									propertyValueCount = 0;
									break;
								}
							}
						}
						if (nextTuple == null) {
							nextTuple = selectNextTuple();
						}
						propertyValueCount++;
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
					if (nextTuple != null) {
						return true;
					} else {
						close();
						return false;
					}
				}

				@Override
				public KvinTuple next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					} else {
						KvinTuple tuple = nextTuple;
						prevTuple = tuple;
						nextTuple = null;
						return tuple;
					}
				}

				@Override
				public void close() {
					if (!closed) {
						try {
							while (!nextTuples.isEmpty()) {
								try {
									nextTuples.poll().getSecond().close();
								} catch (IOException e) {
								}
							}
						} finally {
							readLock.release();
							closed = true;
						}
					}
				}

				KvinTuple convert(GenericRecord record) throws IOException {
					URI p = property != null ? property : getProperty((ByteBuffer) record.get(0));
					return recordToTuple(item, p, context, record);
				}

				void nextReaders() throws IOException {
					folderIndex++;
					List<Path> currentFiles = getDataFiles(dataFolders.get(folderIndex).toString());
					for (Path file : currentFiles) {
						HadoopInputFile inputFile = getFile(file);
						ParquetReader<GenericRecord> reader = createGenericReader(inputFile, FilterCompat.get(filterFinal));
						GenericRecord record = reader.read();
						if (record != null) {
							nextTuples.add(new Pair<>(convert(record), reader));
						} else {
							try {
								reader.close();
							} catch (IOException e) {
							}
						}
					}
				}
			};
		} catch (IOException e) {
			readLock.release();
			throw new UncheckedIOException(e);
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

	private long[] splitRange(String range) {
		String[] minMaxId = range.split("-");
		if (minMaxId.length <= 1) {
			return null;
		}
		return new long[]{Long.parseLong(minMaxId[0]), Long.parseLong(minMaxId[1])};
	}

	private List<Path> getDataFiles(String path) throws IOException {
		return Files.walk(Paths.get(path), 1).skip(1)
				.filter(p -> p.getFileName().toString().startsWith("data__"))
				.map(p -> new Path(p.toString()))
				.collect(Collectors.toList());
	}

	private List<java.nio.file.Path> getDataFolders(IdMappings idMappings) throws IOException {
		long itemId = idMappings.itemId;
		java.nio.file.Path metaPath = Paths.get(archiveLocation, "meta.properties");
		Properties meta;
		try {
			meta = metaCache.get(metaPath, () -> {
				Properties p = new Properties();
				if (Files.exists(metaPath)) {
					p.load(Files.newInputStream(metaPath));
				}
				return p;
			});
		} catch (ExecutionException e) {
			throw new IOException(e);
		}
		return meta.entrySet().stream().flatMap(entry -> {
			String idRange = (String) entry.getValue();
			long[] minMax = splitRange(idRange);
			if (minMax != null && itemId >= minMax[0] && itemId <= minMax[1]) {
				java.nio.file.Path yearFolder = Paths.get(archiveLocation, entry.getKey().toString());
				java.nio.file.Path yearMetaPath = yearFolder.resolve("meta.properties");
				try {
					Properties yearMeta = metaCache.get(yearMetaPath, () -> {
						Properties p = new Properties();
						if (Files.exists(yearMetaPath)) {
							p.load(Files.newInputStream(yearMetaPath));
						}
						return p;
					});
					return yearMeta.entrySet().stream().filter(weekEntry -> {
						String weekIdRange = (String) weekEntry.getValue();
						long[] weekMinMax = splitRange(weekIdRange);
						return weekMinMax != null && itemId >= weekMinMax[0] && itemId <= weekMinMax[1];
					}).map(weekEntry -> yearFolder.resolve(weekEntry.getKey().toString()));
				} catch (Exception e) {
					log.error("Error while loading meta data", e);
				}
			}
			return Stream.empty();
		})
		// sort by year and month descending (recent data first)
		.sorted(Comparator.reverseOrder())
		.collect(Collectors.toList());
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, long limit) {
		return null;
	}

	private KvinTupleMetadata getFirstTuple(URI item, Long itemId, Long propertyId, Long contextId) {
		IdMappings idMappings = null;
		KvinTupleMetadata foundTuple = null;
		try {
			if (itemId == null) {
				idMappings = getIdMappings(item, null, null);
				if (propertyId != null) {
					idMappings.propertyId = propertyId;
				}
				if (contextId != null) {
					idMappings.contextId = contextId;
				}
			} else if (item != null && itemId != null && propertyId != null) {
				idMappings = new IdMappings();
				idMappings.itemId = itemId;
				idMappings.propertyId = propertyId;
				idMappings.contextId = contextId != null ? contextId : 0L;
			}
			if (idMappings.itemId == 0L) {
				return null;
			}

			FilterPredicate filter = generateFetchFilter(idMappings);
			List<java.nio.file.Path> dataFolders = getDataFolders(idMappings);

			KvinTupleInternal firstTuple = null;
			for (java.nio.file.Path dataFolder : dataFolders) {
				for (Path dataFile : getDataFiles(dataFolder.toString())) {
					ParquetReader<KvinTupleInternal> reader = createReader(getFile(dataFile), FilterCompat.get(filter));
					KvinTupleInternal tuple = reader.read();
					if (firstTuple == null || tuple != null && firstTuple != null && tuple.compareTo(firstTuple) < 0) {
						firstTuple = tuple;
					}
					reader.close();
				}
			}

			if (firstTuple != null) {
				URI firstTupleProperty = getProperty(ByteBuffer.wrap(firstTuple.id));
				if (itemId == null) {
					idMappings.propertyId = getId(firstTupleProperty, IdType.PROPERTY_ID);
				}
				foundTuple = new KvinTupleMetadata(item.toString(), firstTupleProperty.toString(),
						idMappings.itemId, idMappings.propertyId, idMappings.contextId);
			}

			return foundTuple;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized IExtendedIterator<URI> properties(URI item) {
		try {
			Lock readLock = readLock();
			return new NiceIterator<>() {
				KvinTupleMetadata currentTuple = getFirstTuple(item, null, null, null);
				KvinTupleMetadata previousTuple = null;

				@Override
				public boolean hasNext() {
					if (currentTuple == null && previousTuple != null) {
						currentTuple = getFirstTuple(URIs.createURI(previousTuple.getItem()), previousTuple.getItemId(),
								previousTuple.getPropertyId() + 1, null);
					}
					if (currentTuple == null) {
						close();
						return false;
					}
					return true;
				}

				@Override
				public URI next() {
					if (currentTuple == null) {
						throw new NoSuchElementException();
					}
					URI property = URIs.createURI(currentTuple.getProperty());
					previousTuple = currentTuple;
					currentTuple = null;
					return property;
				}

				@Override
				public void close() {
					super.close();
					if (readLock.isActive()) {
						readLock.release();
					}
				}
			};
		} catch (IOException e) {
			throw new UncheckedIOException(e);
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

	static class WriterState {
		java.nio.file.Path file;
		ParquetWriter<KvinTupleInternal> writer;
		int year;
		int week;
		long[] minMax = {Long.MAX_VALUE, Long.MIN_VALUE};

		WriterState(java.nio.file.Path file, ParquetWriter<KvinTupleInternal> writer, int year, int week) {
			this.file = file;
			this.writer = writer;
			this.year = year;
			this.week = week;
		}
	}

	static class IdMappings {
		long itemId, propertyId, contextId;
	}

	class WriteContext {
		boolean hasExistingData;
		long itemIdCounter = 0, propertyIdCounter = 0, contextIdCounter = 0;
		Map<String, Long> itemMap = new HashMap<>();
		Map<String, Long> propertyMap = new HashMap<>();
		Map<String, Long> contextMap = new HashMap<>();
	}
}
