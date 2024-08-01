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
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.commons.util.Pair;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
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

import java.io.BufferedWriter;
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
import static io.github.linkedfactory.core.kvin.parquet.Records.encodeRecord;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class KvinParquet implements Kvin {
	static final Logger log = LoggerFactory.getLogger(KvinParquet.class);
	static final long[] EMPTY_IDS = {0};
	static Comparator<GenericRecord> RECORD_COMPARATOR = (a, b) -> {
		int diff = ((ByteBuffer) a.get(0)).compareTo((ByteBuffer) b.get(0));
		if (diff != 0) {
			return diff;
		}
		diff = ((Comparable<Long>) a.get(1)).compareTo((Long) b.get(1));
		if (diff != 0) {
			// time is reverse
			return -diff;
		}
		diff = ((Comparable<Integer>) a.get(2)).compareTo((Integer) b.get(2));
		if (diff != 0) {
			// seqNr is reverse
			return -diff;
		}
		return 0;
	};
	// used by reader
	final Cache<URI, Long> itemIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	final Cache<URI, Long> propertyIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	final Cache<URI, Long> contextIdCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	// Lock
	Map<Path, HadoopInputFile> inputFileCache = new HashMap<>(); // hadoop input file cache
	Cache<Long, URI> propertyIdReverseLookUpCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	Cache<java.nio.file.Path, Properties> metaCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	Cache<java.nio.file.Path, List<Path>> filesCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	String archiveLocation;
	ReadWriteLockManager lockManager = new ReadPrefReadWriteLockManager(true, 5000);

	public KvinParquet(String archiveLocation) {
		this.archiveLocation = archiveLocation;
		if (!this.archiveLocation.endsWith("/")) {
			this.archiveLocation = this.archiveLocation + "/";
		}
		java.nio.file.Path tempPath = Paths.get(archiveLocation, ".tmp");
		try {
			validateAndRepairTempFiles(tempPath);
		} catch (IOException e) {
			log.error("Error while repairing unfinished transaction", e);
		}
	}

	static boolean anyBetween(long[] values, long min, long max) {
		for (int i = 0; i < values.length; i++) {
			if (values[i] >= min && values[i] <= max) {
				return true;
			}
		}
		return false;
	}

	private List<IdMapping> fetchMappingIds(Path mappingFile, FilterPredicate filter) throws IOException {
		List<IdMapping> mappings = null;
		HadoopInputFile inputFile = getFile(mappingFile);
		try (ParquetReader<IdMapping> reader = createReader(inputFile, FilterCompat.get(filter))) {
			while (true) {
				var mapping = reader.read();
				if (mapping != null) {
					if (mappings == null) {
						mappings = new ArrayList<>();
					}
					mappings.add(mapping);
				} else {
					break;
				}
			}
		}
		return mappings == null ? Collections.emptyList() : mappings;
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
			validateAndRepairTempFiles(tempPath);

			writeLock.release();
			writeLock = null;

			Files.createDirectories(tempPath);
			Path itemMappingFile = new Path(tempPath.toString(), "metadata/items__1.parquet");
			Path propertyMappingFile = new Path(tempPath.toString(), "metadata/properties__1.parquet");
			Path contextMappingFile = new Path(tempPath.toString(), "metadata/contexts__1.parquet");

			ParquetWriter<Object> itemMappingWriter = getParquetMappingWriter(itemMappingFile);
			ParquetWriter<Object> propertyMappingWriter = getParquetMappingWriter(propertyMappingFile);
			ParquetWriter<Object> contextMappingWriter = getParquetMappingWriter(contextMappingFile);

			WriterState writerState = null;
			String prevKey = null;
			KvinTupleInternal prevTuple = null;
			for (KvinTuple tuple : tuples) {
				KvinTupleInternal internalTuple = new KvinTupleInternal();

				Calendar tupleDate = getDate(tuple.time);
				int year = tupleDate.get(Calendar.YEAR);
				int week = tupleDate.get(Calendar.WEEK_OF_YEAR);

				String key = year + "_" + week;
				if (!key.equals(prevKey)) {
					writerState = writers.get(key);
					if (writerState == null) {
						String yearFolderName = String.format("%04d", year);
						String weekFolderName = String.format("%02d", week);
						java.nio.file.Path file = tempPath.resolve(yearFolderName)
								.resolve(weekFolderName)
								.resolve("data__1.parquet");
						Files.createDirectories(file.getParent());
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
				// set first flag
				if (prevTuple == null || !Arrays.equals(prevTuple.id, internalTuple.id)) {
					internalTuple.setFirst(true);
				}
				writerState.writer.write(internalTuple);
				writerState.minMax[0] = Math.min(writerState.minMax[0], writeContext.lastItemId);
				writerState.minMax[1] = Math.max(writerState.minMax[1], writeContext.lastItemId);
				prevTuple = internalTuple;
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

			var metaPath = Paths.get(archiveLocation, "meta.properties");
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

			Map<Integer, List<WriterState>> writersPerYear = writers.values().stream()
					.collect(Collectors.groupingBy(s -> s.year));
			for (Map.Entry<Integer, List<WriterState>> entry : writersPerYear.entrySet()) {
				int year = entry.getKey();
				String yearFolderName = String.format("%04d", year);
				long[] minMaxYear = minMaxYears.get(year);
				meta.put(yearFolderName, minMaxYear[0] + "-" + minMaxYear[1]);
			}

			meta.store(Files.newOutputStream(tempPath.resolve("meta.properties")), null);
			createMetaFiles(tempPath, writersPerYear);

			java.nio.file.Path validPath = tempPath.resolve("valid");
			try (BufferedWriter writer = Files.newBufferedWriter(validPath)) {
				writer.write(String.valueOf(System.currentTimeMillis()));
			}

			writeLock = writeLock();
			moveTempFiles(tempPath);
		} catch (Throwable e) {
			log.error("Error while adding data", e);
		} finally {
			if (writeLock != null) {
				writeLock.release();
			}
			Thread.currentThread().setContextClassLoader(contextCl);
		}
	}

	private void moveTempFiles(java.nio.file.Path tempPath) throws IOException {
		moveDataFiles(tempPath);
		moveMappingFiles(tempPath);
		deleteTempFiles(tempPath);
		clearCaches();
	}

	private void deleteTempFiles(java.nio.file.Path tempPath) throws IOException {
		// completely delete temporary directory
		Files.walk(tempPath).sorted(Comparator.reverseOrder())
				.map(java.nio.file.Path::toFile)
				.forEach(File::delete);
	}

	private void validateAndRepairTempFiles(java.nio.file.Path tempPath) throws IOException {
		if (Files.exists(tempPath)) {
			if (Files.exists(tempPath.resolve("valid"))) {
				moveTempFiles(tempPath);
			} else {
				deleteTempFiles(tempPath);
			}
		}
	}

	private void moveMappingFiles(java.nio.file.Path tempPath) throws IOException {
		java.nio.file.Path metadataPath = Paths.get(archiveLocation, "metadata");
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
	}

	private void moveDataFiles(java.nio.file.Path source) throws IOException {
		java.nio.file.Path destination = Paths.get(archiveLocation);
		Files.walk(source)
				.skip(1)
				.filter(p -> Files.isRegularFile(p))
				.forEach(sourceFile -> {
					java.nio.file.Path dest = destination.resolve(source.relativize(sourceFile)).getParent();
					try {
						java.nio.file.Path destFile = null;
						if (sourceFile.getFileName().toString().startsWith("data__")) {
							Files.createDirectories(dest);
							int maxSeqNr = Files.list(dest).map(p -> {
								String name = p.getFileName().toString();
								if (name.startsWith("data__")) {
									Matcher m = fileWithSeqNr.matcher(name);
									if (m.matches()) {
										return Integer.parseInt(m.group(2));
									}
								}
								return 0;
							}).max(Integer::compareTo).orElse(0);
							String filename = "data__" + (maxSeqNr + 1) + ".parquet";
							destFile = dest.resolve(filename);
						} else if (sourceFile.getFileName().toString().startsWith("meta.properties")) {
							destFile = dest.resolve(sourceFile.getFileName());
							Files.deleteIfExists(destFile);
						}
						if (destFile != null) {
							log.debug("moving: " + sourceFile + " -> " + destFile);
							Files.createDirectories(destFile.getParent());
							Files.move(sourceFile, destFile);
						}
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				});
	}

	private void createMetaFiles(java.nio.file.Path tempPath, Map<Integer, List<WriterState>> writersPerYear) throws IOException {
		for (Map.Entry<Integer, List<WriterState>> entry : writersPerYear.entrySet()) {
			int year = entry.getKey();
			String yearFolderName = String.format("%04d", year);
			java.nio.file.Path yearFolder = Paths.get(archiveLocation, yearFolderName);

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
			}
			var tempYearFolder = tempPath.resolve(yearFolderName);
			Files.createDirectories(tempYearFolder);
			yearMeta.store(Files.newOutputStream(tempYearFolder.resolve("meta.properties")), null);
		}
	}

	public void clearCaches() {
		// clear cache with meta data
		metaCache.invalidateAll();
		filesCache.invalidateAll();
		inputFileCache.clear();

		// invalidate id caches - TODO could be improved by directly updating the caches
		itemIdCache.invalidateAll();
		propertyIdCache.invalidateAll();
		contextIdCache.invalidateAll();
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
					writeContext.lastItemId = id;
					return id;
				}
			}
			long newId = ++writeContext.itemIdCounter;
			writeContext.lastItemId = newId;
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
		idBuffer.putLong(contextId);
		idBuffer.putLong(propertyId);
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
					var mappings = fetchMappingIds(new Path(mappingFile.getPath()), filter);
					if (!mappings.isEmpty()) {
						mapping = mappings.get(0);
						break;
					}
				}
				return mapping != null ? mapping.getId() : 0L;
			});
		} catch (ExecutionException e) {
			return 0L;
		}
		return id != null ? id : 0L;
	}

	private long[] getIds(List<URI> entities, IdType idType) {
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
		long[] ids = new long[entities.size()];
		int i = 0;

		var cachedIds = idCache.getAllPresent(entities);
		Map<String, Integer> toFetch = new HashMap<>();
		for (URI entity : entities) {
			Long id = cachedIds.get(entity);
			if (id != null) {
				ids[i] = id;
			} else {
				toFetch.put(entity.toString(), i);
			}
			i++;
		}
		if (!toFetch.isEmpty()) {
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
			FilterPredicate filter = in(FilterApi.binaryColumn("value"),
					toFetch.keySet().stream().map(k -> Binary.fromString(k)).collect(Collectors.toSet()));
			File[] mappingFiles = new File(this.archiveLocation + "metadata/").listFiles((file, s) -> s.startsWith(name));
			if (mappingFiles == null) {
				return ids;
			}
			try {
				for (File mappingFile : mappingFiles) {
					var mappings = fetchMappingIds(new Path(mappingFile.getPath()), filter);
					for (IdMapping mapping : mappings) {
						ids[toFetch.get(mapping.getValue())] = mapping.getId();
						idCache.put(URIs.createURI(mapping.getValue()), mapping.getId());
					}
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
		return ids;
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

	private FilterPredicate generateFetchFilter(long[] itemIds, long[] propertyIds, long contextId) {
		FilterPredicate filter = null;
		for (long itemId : itemIds) {
			if (itemId == 0L) {
				continue;
			}
			for (long propertyId : propertyIds) {
				if (propertyId == 0L && propertyIds != EMPTY_IDS) {
					continue;
				}
				var fetchFilter = createIdFilter(itemId, propertyId, contextId);
				filter = filter == null ? fetchFilter : or(filter, fetchFilter);
			}
		}
		return filter;
	}

	private FilterPredicate createIdFilter(long itemId, long propertyId, long contextId) {
		if (itemId != 0L && propertyId != 0L && contextId != 0L) {
			ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES * 3);
			keyBuffer.putLong(itemId);
			keyBuffer.putLong(contextId);
			keyBuffer.putLong(propertyId);
			return eq(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(keyBuffer.array()));
		} else if (contextId != 0L) {
			ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES * 2);
			keyBuffer.putLong(itemId);
			keyBuffer.putLong(contextId);
			return and(gt(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(keyBuffer.array())),
					lt(FilterApi.binaryColumn("id"),
							Binary.fromConstantByteArray(ByteBuffer.allocate(Long.BYTES * 2)
									.putLong(itemId).putLong(contextId + 1).array())));
		} else {
			ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES);
			keyBuffer.putLong(itemId);
			return and(gt(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(keyBuffer.array())),
					lt(FilterApi.binaryColumn("id"),
							Binary.fromConstantByteArray(ByteBuffer.allocate(Long.BYTES)
									.putLong(itemId + 1).array())));
		}
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(List<URI> items, List<URI> properties, URI context, long end, long begin, long limit, long interval, String op) {
		try {
			IExtendedIterator<KvinTuple> internalResult = fetchInternal(items, properties, context, end, begin, limit);
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

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		try {
			return fetchInternal(List.of(item), property == null ? List.of() : List.of(property),
					context, null, null, limit);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
		var properties = property == null ? Collections.<URI>emptyList() : List.of(property);
		return fetch(List.of(item), properties, context, end, begin, limit, interval, op);
	}

	public URI getProperty(long propertyId) throws IOException {
		URI cachedProperty = propertyIdReverseLookUpCache.getIfPresent(propertyId);
		if (cachedProperty == null) {
			FilterPredicate filter = eq(FilterApi.longColumn("id"), propertyId);
			Path metadataFolder = new Path(this.archiveLocation + "metadata/");
			File[] mappingFiles = new File(metadataFolder.toString()).listFiles((file, s) -> s.startsWith("properties"));
			IdMapping propertyMapping = null;

			for (File mappingFile : mappingFiles) {
				var mappings = fetchMappingIds(new Path(mappingFile.getPath()), filter);
				if (!mappings.isEmpty()) {
					propertyMapping = mappings.get(0);
					break;
				}
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

	private IExtendedIterator<KvinTuple> fetchInternal(List<URI> items, List<URI> properties, URI context, Long end, Long begin, Long limit) throws IOException {
		Lock readLock = readLock();
		try {
			URI contextFinal = context != null ? context : Kvin.DEFAULT_CONTEXT;
			long[] itemIds = getIds(items, IdType.ITEM_ID);
			long[] propertyIds = properties.isEmpty() ? EMPTY_IDS : getIds(properties, IdType.PROPERTY_ID);
			long contextId = 0;
			if (context != null) {
				contextId = getId(context, IdType.CONTEXT_ID);
			}
			if (contextId == 0L) {
				// ensure read lock is freed
				readLock.release();
				return NiceIterator.emptyIterator();
			}
			// filters
			FilterPredicate filter = generateFetchFilter(itemIds, propertyIds, contextId);
			if (filter == null) {
				// ensure read lock is freed
				readLock.release();
				return NiceIterator.emptyIterator();
			}
			if (begin != null) {
				filter = and(filter, gtEq(FilterApi.longColumn("time"), begin));
			}
			if (end != null) {
				filter = and(filter, lt(FilterApi.longColumn("time"), end));
			}

			final FilterPredicate filterFinal = filter;
			List<java.nio.file.Path> dataFolders = getDataFolders(itemIds);
			if (dataFolders.isEmpty()) {
				// ensure read lock is freed
				readLock.release();
				return NiceIterator.emptyIterator();
			}
			return new NiceIterator<KvinTuple>() {
				final PriorityQueue<Pair<GenericRecord, ParquetReader<GenericRecord>>> nextTuples =
						new PriorityQueue<>(Comparator.comparing(Pair::getFirst, RECORD_COMPARATOR));
				GenericRecord prevRecord;
				KvinTuple nextTuple;
				long propertyValueCount;
				int folderIndex = -1;
				boolean closed;

				{
					try {
						nextReaders();
						if (properties.isEmpty()) {
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
					boolean skipAfterLimit = limit != 0 && propertyValueCount >= limit;

					KvinTuple tuple = null;
					while (tuple == null) {
						while (nextTuples.isEmpty() && folderIndex < dataFolders.size() - 1) {
							nextReaders();
						}
						var min = nextTuples.isEmpty() ? null : nextTuples.poll();
						if (min != null) {
							// omit duplicates in terms of id, time, and seqNr
							boolean isDuplicate = prevRecord != null && RECORD_COMPARATOR.compare(prevRecord, min.getFirst()) == 0;
							if (!isDuplicate) {
								if (skipAfterLimit) {
									// reset value count if property changes
									if (!min.getFirst().get(0).equals(prevRecord.get(0))) {
										skipAfterLimit = false;
										propertyValueCount = 0;
									}
								}
								if (!skipAfterLimit) {
									prevRecord = min.getFirst();
									tuple = convert(min.getFirst());
									propertyValueCount++;
								}
							}
							var record = min.getSecond().read();
							if (record != null) {
								nextTuples.add(new Pair<>(record, min.getSecond()));
							} else {
								try {
									min.getSecond().close();
								} catch (IOException e) {
								}
							}
						} else {
							break;
						}
					}
					return tuple;
				}

				@Override
				public boolean hasNext() {
					if (nextTuple != null) {
						return true;
					}
					try {
						nextTuple = selectNextTuple();
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
					var itemId = ((ByteBuffer) record.get(0)).getLong(0);
					// skip item and context ids
					var propertyId = ((ByteBuffer) record.get(0)).getLong(Long.BYTES * 2);
					URI item = null;
					URI property = null;
					for (int i = 0; i < itemIds.length; i++) {
						if (itemIds[i] == itemId) {
							item = items.get(i);
							break;
						}
					}
					if (!properties.isEmpty()) {
						for (int i = 0; i < propertyIds.length; i++) {
							if (propertyIds[i] == propertyId) {
								property = properties.get(i);
								break;
							}
						}
					}
					return recordToTuple(item, property != null ? property : getProperty(propertyId), contextFinal, record);
				}

				void nextReaders() throws IOException {
					folderIndex++;
					List<Path> currentFiles = getDataFiles(dataFolders.get(folderIndex).toString());
					for (Path file : currentFiles) {
						HadoopInputFile inputFile = getFile(file);
						ParquetReader<GenericRecord> reader = createGenericReader(inputFile, FilterCompat.get(filterFinal));
						GenericRecord record = reader.read();
						if (record != null) {
							nextTuples.add(new Pair<>(record, reader));
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
	public boolean delete(URI item, URI context) {
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
		try {
			return filesCache.get(Paths.get(path), () -> Files.walk(Paths.get(path), 1).skip(1)
					.filter(p -> p.getFileName().toString().startsWith("data__"))
					.map(p -> new Path(p.toString()))
					.collect(Collectors.toList()));
		} catch (ExecutionException e) {
			throw new IOException(e);
		}
	}

	private List<java.nio.file.Path> getDataFolders(long[] itemIds) throws IOException {
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
					if (minMax != null && anyBetween(itemIds, minMax[0], minMax[1])) {
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
								return weekMinMax != null && anyBetween(itemIds, weekMinMax[0], weekMinMax[1]);
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
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		return null;
	}

	private List<URI> getProperties(long itemId, long contextId) {
		try {
			ByteBuffer lowKey = ByteBuffer.allocate(Long.BYTES * 2);
			lowKey.putLong(itemId);
			lowKey.putLong(contextId);
			ByteBuffer highKey = ByteBuffer.allocate(Long.BYTES * 3);
			highKey.putLong(itemId);
			highKey.putLong(contextId);
			highKey.putLong(Long.MAX_VALUE);
			FilterPredicate filter = and(eq(FilterApi.booleanColumn("first"), true), and(
					gt(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(lowKey.array())),
					lt(FilterApi.binaryColumn("id"), Binary.fromConstantByteArray(highKey.array()))));

			List<java.nio.file.Path> dataFolders = getDataFolders(new long[]{itemId});
			Set<Long> propertyIds = new LinkedHashSet<>();

			for (java.nio.file.Path dataFolder : dataFolders) {
				for (Path dataFile : getDataFiles(dataFolder.toString())) {
					ParquetReader<GenericRecord> reader = createGenericReader(getFile(dataFile), FilterCompat.get(filter));
					GenericRecord record;
					while ((record = reader.read()) != null) {
						ByteBuffer idBb = (ByteBuffer) record.get(0);
						// skip item id
						idBb.getLong();
						// skip context id
						idBb.getLong();
						long currentPropertyId = idBb.getLong();
						propertyIds.add(currentPropertyId);
					}
					reader.close();
				}
			}

			return propertyIds.stream().map(id -> {
				try {
					return getProperty(id);
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}).collect(Collectors.toList());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		if (context == null) {
			context = Kvin.DEFAULT_CONTEXT;
		}
		Lock readLock = null;
		try {
			readLock = readLock();
			IdMappings idMappings = getIdMappings(item, null, context);
			if (idMappings.itemId == 0L || idMappings.contextId == 0L) {
				return NiceIterator.emptyIterator();
			}
			List<URI> properties = getProperties(idMappings.itemId, idMappings.contextId);
			return WrappedIterator.create(properties.iterator());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} finally {
			readLock.release();
		}
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
		long lastItemId = 0;
		Map<String, Long> itemMap = new HashMap<>();
		Map<String, Long> propertyMap = new HashMap<>();
		Map<String, Long> contextMap = new HashMap<>();
	}
}
