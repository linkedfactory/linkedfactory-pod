package io.github.linkedfactory.core.kvin.parquet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.parquet.records.KvinRecordConverter;
import io.github.linkedfactory.core.kvin.parquet.records.KvinRecord;
import io.github.linkedfactory.core.kvin.parquet.records.SimpleGroupExt;
import io.github.linkedfactory.core.kvin.util.AggregatingIterator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.commons.util.Pair;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
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
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
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
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
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
	static Comparator<KvinRecord> KVIN_RECORD_COMPARATOR = (a, b) -> {
		int diff = (int) (a.itemId - b.itemId);
		if (diff != 0) {
			return diff;
		}
		diff = (int) (a.propertyId - b.propertyId);
		if (diff != 0) {
			return diff;
		}
		diff = (int) (a.time - b.time);
		if (diff != 0) {
			// time is reverse
			return -diff;
		}
		diff = a.seqNr - b.seqNr;
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
	final Cache<Pair<Path, Integer>, ColumnIndexStore> indexCache = CacheBuilder.newBuilder().maximumSize(10000).build();

	// Lock
	final Map<Path, InputFileInfo> inputFileCache = new HashMap<>(); // hadoop input file cache
	final Cache<Long, URI> propertyIdReverseLookUpCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	final Cache<java.nio.file.Path, Properties> metaCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	final Cache<java.nio.file.Path, List<Path>> filesCache = CacheBuilder.newBuilder().maximumSize(10000).build();
	final ReadWriteLockManager lockManager = new ReadPrefReadWriteLockManager(true, 5000);
	String archiveLocation;
	Duration retentionPeriod;

	public KvinParquet(String archiveLocation) {
		this(archiveLocation, null);
	}

	public KvinParquet(String archiveLocation, Duration retentionPeriod) {
		this.archiveLocation = archiveLocation;
		this.retentionPeriod = retentionPeriod;
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
		HadoopInputFile inputFile = getFile(mappingFile).file;
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
				InputFileInfo inputFile = getFile(new Path(metadataPath.resolve(mappingFile.getFirst()).toString()));
				for (BlockMetaData blockMeta : inputFile.metadata.getBlocks()) {
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

	private InputFileInfo getFile(Path path) throws IOException {
		InputFileInfo inputFileInfo;
		synchronized (inputFileCache) {
			inputFileInfo = inputFileCache.get(path);
			if (inputFileInfo == null) {
				HadoopInputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
				ParquetReadOptions.Builder optionsBuilder = HadoopReadOptions.builder(configuration, path);
				var options = optionsBuilder.build();
				ParquetMetadata metadata = ParquetFileReader.readFooter(inputFile, options, inputFile.newStream());
				inputFileInfo = new InputFileInfo(path, inputFile, metadata);
				inputFileCache.put(path, inputFileInfo);
			}
		}
		return inputFileInfo;
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
			for (KvinTuple tuple : tuples) {
				KvinRecord record = new KvinRecord();

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
						writerState = new WriterState(file, getKvinRecordWriter(new Path(file.toString())),
								year, week);
						writers.put(key, writerState);
					}
					prevKey = key;
				}

				// writing mappings and values
				long[] id = generateIds(tuple, writeContext,
						itemMappingWriter, propertyMappingWriter, contextMappingWriter);
				record.itemId = id[0];
				record.contextId = id[1];
				record.propertyId = id[2];
				record.time = tuple.time;
				record.seqNr = tuple.seqNr;

				Object value = tuple.value;
				if (value instanceof Record || value instanceof URI || value instanceof BigInteger ||
						value instanceof BigDecimal || value instanceof Short || value instanceof Object[]) {
					value = ByteBuffer.wrap(encodeRecord(value));
				}
				record.value = value;

				writerState.writer.write(record);
				writerState.minMax[0] = Math.min(writerState.minMax[0], writeContext.lastItemId);
				writerState.minMax[1] = Math.max(writerState.minMax[1], writeContext.lastItemId);
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
			if (!meta.isEmpty()) {
				meta.store(Files.newOutputStream(tempPath.resolve("meta.properties")), null);
			}
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
				.filter(Files::isRegularFile)
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
		// clear caches with meta data
		indexCache.invalidateAll();
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

	private long[] generateIds(KvinTuple tuple,
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
		return new long[]{itemId, contextId, propertyId};
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
			return and(eq(FilterApi.longColumn("propertyId"), propertyId),
					and(eq(FilterApi.longColumn("itemId"), itemId),
							eq(FilterApi.longColumn("contextId"), contextId)));
		} else if (contextId != 0L) {
			return and(eq(FilterApi.longColumn("itemId"), itemId),
					eq(FilterApi.longColumn("contextId"), contextId));
		} else {
			return eq(FilterApi.longColumn("itemId"), itemId);
		}
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(List<URI> items, List<URI> properties, URI context, long end, long begin, long limit, long interval, String op) {
		try {
			IExtendedIterator<KvinTuple> internalResult = fetchInternal(items, properties, context, end, begin, op == null ? limit : 0L);
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

	private IExtendedIterator<KvinRecord> createKvinRecordReader(InputFileInfo fileInfo, FilterCompat.Filter filter) throws IOException {
		ParquetReadOptions.Builder optionsBuilder = HadoopReadOptions.builder(configuration, fileInfo.path);
		optionsBuilder.withAllocator(new HeapByteBufferAllocator());
		optionsBuilder.withRecordFilter(filter);
		ParquetReadOptions options = optionsBuilder.build();
		ParquetFileReader r = new ParquetFileReader(configuration, fileInfo.path, fileInfo.metadata, options
				/* , new MemoryMappedSeekableInputStream(Paths.get(fileInfo.path.toString())) */) {
			static Field blocksField;

			static {
				try {
					blocksField = ParquetFileReader.class.getDeclaredField("blocks");
					// make it accessible
					blocksField.setAccessible(true);
				} catch (NoSuchFieldException e) {
					// ignore
				}
			}

			@Override
			public ColumnIndexStore getColumnIndexStore(int blockIndex) {
				if (blocksField != null) {
					try {
						BlockMetaData block = (BlockMetaData) ((List<?>) blocksField.get(this)).get(blockIndex);
						return indexCache.get(new Pair<>(fileInfo.path, block.getOrdinal()), () -> {
							Map<ColumnPath, ColumnIndex> columnIndexes = new HashMap<>();
							Map<ColumnPath, OffsetIndex> offsetIndexes = new HashMap<>();
							for (ColumnChunkMetaData columnChunkMetaData : block.getColumns()) {
								columnIndexes.put(columnChunkMetaData.getPath(), readColumnIndex(columnChunkMetaData));
								offsetIndexes.put(columnChunkMetaData.getPath(), readOffsetIndex(columnChunkMetaData));
							}
							return new ColumnIndexStore() {
								@Override
								public ColumnIndex getColumnIndex(ColumnPath column) {
									return columnIndexes.get(column);
								}

								@Override
								public OffsetIndex getOffsetIndex(ColumnPath column) throws MissingOffsetIndexException {
									return offsetIndexes.get(column);
								}
							};
						});
					} catch (IllegalAccessException | ExecutionException e) {
						log.error("Error while creating index store", e);
					}
				}
				return super.getColumnIndexStore(blockIndex);
			}
		};
		return new NiceIterator<>() {
			RecordReader recordReader;
			KvinRecord next;
			PageReadStore pages;
			long readRows;

			@Override
			public boolean hasNext() {
				if (next == null) {
					try {
						while (next == null) {
							if (pages == null || readRows == pages.getRowCount()) {
								readRows = 0;
								recordReader = null;
								if (pages != null) {
									pages.close();
								}
								pages = r.readNextFilteredRowGroup();
								if (pages != null && pages.getRowCount() > 0) {
									recordReader = fileInfo.columnIO.getRecordReader(pages,
											new KvinRecordConverter(), filter);
								}
							}
							if (recordReader != null) {
								while (readRows < pages.getRowCount()) {
									next = (KvinRecord) recordReader.read();
									readRows++;
									if (!recordReader.shouldSkipCurrentRecord()) {
										break;
									}
								}
							} else {
								r.close();
								break;
							}
						}
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					} catch (Exception e) {
						log.error("Error while reading next element", e);
						close();
						return false;
					}
				}
				return next != null;
			}

			@Override
			public KvinRecord next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				KvinRecord result = next;
				next = null;
				return result;
			}

			@Override
			public void close() {
				if (pages != null) {
					pages.close();
				}
				try {
					r.close();
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}
		};
	}

	private IExtendedIterator<KvinTuple> fetchInternal(List<URI> items, List<URI> properties, URI context, Long end, Long begin, Long limit) throws IOException {
		ClassLoader contextCl = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(KvinParquet.class.getClassLoader());
			if (items.size() == 1 && limit != null && limit > 0L) {
				// this optimizes the case where data needs to be skipped due to a limit as this is currently not
				// achievable with filters
				if (properties.isEmpty()) {
					properties = properties(items.get(0), context).toList();
				}
				IExtendedIterator<KvinTuple> it = NiceIterator.emptyIterator();
				for (URI property : properties) {
					// use lazy initialization for further iterators
					it = it.andThen(new NiceIterator<>() {
						IExtendedIterator<KvinTuple> base;

						@Override
						public boolean hasNext() {
							if (base == null) {
								try {
									base = doFetch(items, Collections.singletonList(property), context, end, begin, limit);
								} catch (IOException e) {
									throw new UncheckedIOException(e);
								}
							}
							return base.hasNext();
						}

						@Override
						public KvinTuple next() {
							ensureHasNext();
							return base.next();
						}

						@Override
						public void close() {
							base.close();
						}
					});
				}
				return it;
			} else {
				return doFetch(items, properties, context, end, begin, limit);
			}
		} finally {
			Thread.currentThread().setContextClassLoader(contextCl);
		}
	}

	private IExtendedIterator<KvinTuple> doFetch(List<URI> items, List<URI> properties, URI context, Long end, Long begin, Long limit) throws IOException {
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
				filter = and(gtEq(FilterApi.longColumn("time"), begin), filter);
			}
			if (end != null) {
				filter = and(ltEq(FilterApi.longColumn("time"), end), filter);
			}

			final FilterPredicate filterFinal = filter;
			List<java.nio.file.Path> dataFolders = getDataFolders(itemIds,
					end != null ? getDate(end) : null,
					begin != null ? getDate(begin) : null);
			if (dataFolders.isEmpty()) {
				// ensure read lock is freed
				readLock.release();
				return NiceIterator.emptyIterator();
			}
			return new NiceIterator<KvinTuple>() {
				final PriorityQueue<Pair<KvinRecord, IExtendedIterator<KvinRecord>>> nextTuples =
						new PriorityQueue<>(Comparator.comparing(Pair::getFirst, KVIN_RECORD_COMPARATOR));
				KvinRecord prevRecord;
				KvinTuple nextTuple;
				long propertyValueCount;
				int folderIndex = -1;
				boolean closed;
				URI lastItem, lastProperty;
				long lastItemId, lastPropertyId;

				{
					nextReaders();
					if (properties.isEmpty()) {
						// directly load all relevant files if property is not given
						// as data might be distributed over multiple files for one property
						while (folderIndex < dataFolders.size() - 1) {
							nextReaders();
						}
					}
				}

				KvinTuple selectNextTuple() throws IOException {
					boolean skipAfterLimit = limit != 0 && propertyValueCount >= limit;
					if (skipAfterLimit && itemIds.length == 1 && propertyIds != EMPTY_IDS && propertyIds.length == 1) {
						// we are finished, if only one item and one property is requested
						return null;
					}

					KvinTuple tuple = null;
					while (tuple == null) {
						while (nextTuples.isEmpty() && folderIndex < dataFolders.size() - 1) {
							nextReaders();
						}
						var min = nextTuples.isEmpty() ? null : nextTuples.poll();
						if (min != null) {
							// omit duplicates in terms of id, time, and seqNr
							boolean isDuplicate = prevRecord != null && KVIN_RECORD_COMPARATOR.compare(prevRecord, min.getFirst()) == 0;
							if (!isDuplicate) {
								if (skipAfterLimit) {
									// reset value count if property changes
									KvinRecord nextMin = min.getFirst();
									if (nextMin.itemId != prevRecord.itemId || nextMin.propertyId != prevRecord.propertyId) {
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
							if (min.getSecond().hasNext()) {
								nextTuples.add(new Pair<>(min.getSecond().next(), min.getSecond()));
							} else {
								min.getSecond().close();
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
								nextTuples.poll().getSecond().close();
							}
						} finally {
							readLock.release();
							closed = true;
						}
					}
				}

				KvinTuple convert(KvinRecord record) throws IOException {
					var itemId = record.itemId;
					// skip item and context ids
					var propertyId = record.propertyId;
					if (itemId != lastItemId) {
						lastItemId = itemId;
						for (int i = 0; i < itemIds.length; i++) {
							if (itemIds[i] == itemId) {
								lastItem = items.get(i);
								break;
							}
						}
					}
					if (propertyId != lastPropertyId) {
						lastPropertyId = propertyId;
						if (!properties.isEmpty()) {
							for (int i = 0; i < propertyIds.length; i++) {
								if (propertyIds[i] == propertyId) {
									lastProperty = properties.get(i);
									break;
								}
							}
						} else {
							lastProperty = getProperty(propertyId);
						}
					}
					return recordToTuple(lastItem, lastProperty, contextFinal, record);
				}

				void nextReaders() {
					folderIndex++;
					String path = dataFolders.get(folderIndex).toString();
					try {
						List<Path> currentFiles = getDataFiles(path);
						for (Path file : currentFiles) {
							try {
								IExtendedIterator<KvinRecord> reader = createKvinRecordReader(getFile(file), FilterCompat.get(filterFinal));
								if (reader.hasNext()) {
									nextTuples.add(new Pair<>(reader.next(), reader));
								} else {
									reader.close();
								}
							} catch (IOException e) {
								log.warn("Error while loading data from parquet file {}", file);
							}
						}
					} catch (IOException e) {
						log.warn("Error while loading files from folder {}", path);
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
		return getDataFolders(itemIds, null, null);
	}

	private List<java.nio.file.Path> getDataFolders(long[] itemIds, Calendar end, Calendar begin) throws IOException {
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
					// exclude year folders that are outside of begin and end time range
					int year = begin != null || end != null ? Integer.parseInt(entry.getKey().toString()) : -1;
					if (begin != null && year < begin.get(Calendar.YEAR) ||
							end != null && year > end.get(Calendar.YEAR)) {
						return Stream.empty();
					}

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
								// exclude week folders that are outside of begin and end time range
								if (begin != null || end != null) {
									int week = Integer.parseInt(weekEntry.getKey().toString());
									if (begin != null && year == begin.get(Calendar.YEAR) && week < begin.get(Calendar.WEEK_OF_YEAR) ||
											end != null && year == end.get(Calendar.YEAR) && week > end.get(Calendar.WEEK_OF_YEAR)) {
										return false;
									}
								}

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
		return NiceIterator.emptyIterator();
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		return NiceIterator.emptyIterator();
	}

	private List<URI> getProperties(long itemId, long contextId) {
		try {
			FilterPredicate filter = and(eq(FilterApi.booleanColumn("first"), true),
					createIdFilter(itemId, 0L, contextId));

			List<java.nio.file.Path> dataFolders = getDataFolders(new long[]{itemId});
			Set<Long> propertyIds = new LinkedHashSet<>();

			for (java.nio.file.Path dataFolder : dataFolders) {
				try {
					for (Path dataFile : getDataFiles(dataFolder.toString())) {
						try {
							var reader = createKvinRecordReader(getFile(dataFile), FilterCompat.get(filter));
							while (reader.hasNext()) {
								var record = reader.next();
								long currentPropertyId = record.propertyId;
								propertyIds.add(currentPropertyId);
							}
							reader.close();
						} catch (IOException e) {
							log.warn("Error while loading data from parquet file {}", dataFile);
						}
					}
				} catch (IOException e) {
					log.warn("Error while loading files from folder {}", dataFolder);
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

	/**
	 * Cleans up archive according to retention period.
	 *
	 * @throws IOException
	 */
	public void cleanUp() throws IOException {
		var writeLock = writeLock();
		try {
			cleanUpInternal();
		} finally {
			writeLock.release();
		}
	}

	protected void cleanUpInternal() throws IOException {
		if (this.retentionPeriod != null && Files.exists(Paths.get(archiveLocation))) {
			log.info("remove parquet files older than {} days", retentionPeriod.toDays());
			var maxAge = Instant.now().minus(retentionPeriod).atZone(ZoneId.of("UTC"));

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

			var metaNew = new Properties();
			var metaChanged = false;
			for (var yearEntry : meta.entrySet()) {
				// exclude year folders that newer than max age
				int year = Integer.parseInt(yearEntry.getKey().toString());
				if (year > maxAge.get(ChronoField.YEAR)) {
					continue;
				}

				long[] minMaxYearNew = new long[]{Long.MAX_VALUE, Long.MIN_VALUE};
				var yearFolder = Paths.get(archiveLocation, yearEntry.getKey().toString());
				var yearMetaPath = yearFolder.resolve("meta.properties");
				if (Files.exists(yearFolder)) {
					Properties yearMeta;
					try {
						yearMeta = metaCache.get(yearMetaPath, () -> {
							Properties p = new Properties();
							if (Files.exists(yearMetaPath)) {
								p.load(Files.newInputStream(yearMetaPath));
							}
							return p;
						});
					} catch (Exception e) {
						log.error("Error while loading meta data", e);
						yearMeta = new Properties();
					}

					var yearMetaNew = new Properties();
					boolean yearChanged = false;
					for (var weekEntry : yearMeta.entrySet()) {
						// exclude week folders that are newer than max age
						int week = Integer.parseInt(weekEntry.getKey().toString());

						if (year == maxAge.get(ChronoField.YEAR) && week > maxAge.get(ChronoField.ALIGNED_WEEK_OF_YEAR)) {
							long[] minMaxWeek = splitRange((String) yearEntry.getValue());
							minMaxYearNew[0] = Math.min(minMaxWeek[0], minMaxYearNew[0]);
							minMaxYearNew[1] = Math.max(minMaxWeek[1], minMaxYearNew[1]);

							yearMetaNew.put(weekEntry.getKey(), weekEntry.getValue());
							continue;
						}

						// remove week entry from metadata
						yearChanged = true;

						var weekFolder = yearFolder.resolve(weekEntry.getKey().toString());
						if (Files.exists(weekFolder)) {
							// recursively delete all data files within week folder
							try (var paths = Files.walk(weekFolder)) {
								paths.sorted(Comparator.reverseOrder()).forEach(p -> {
									try {
										Files.delete(p);
									} catch (IOException e) {
										log.error("Error while deleting archive file or folder", p);
									}
								});
							}
						}
					}
					if (yearMeta.isEmpty()) {
						Files.delete(yearFolder);
						metaChanged = true;
					} else {
						if (yearChanged) {
							// store updated metadata file for this year
							yearMetaNew.store(Files.newOutputStream(yearMetaPath), null);
							// update id range for year
							metaNew.put(yearEntry.getKey(), minMaxYearNew[0] + "-" + minMaxYearNew[1]);
							metaChanged = true;
						} else {
							// keep existing entry
							metaNew.put(yearEntry.getKey(), yearEntry.getValue());
						}
					}
				}
			}

			if (metaChanged) {
				// update top-level metadata
				metaNew.store(Files.newOutputStream(metaPath), null);

				// clear caches
				clearCaches();
			}
		}
	}

	// id enum
	enum IdType {
		ITEM_ID,
		PROPERTY_ID,
		CONTEXT_ID
	}

	static class GroupRecord {
		final ByteBuffer id;
		final Group group;
		final int row;

		GroupRecord(Group group, int row) {
			this.group = group;
			this.id = group.getBinary(0, 0).toByteBuffer();
			this.row = row;
		}

		public Object get(int i) {
			var value = ((SimpleGroupExt) group).getObject(i, row);
			var type = group.getType().getType(i);
			if (value instanceof Binary) {
				var annotation = type.getLogicalTypeAnnotation();
				if (annotation != null && "STRING".equals(annotation.toString())) {
					return new String(((Binary) value).getBytes(), StandardCharsets.UTF_8);
				}
			}
			return value;
		}

		public Object getFirstNonNull(int startIndex) {
			var index = ((SimpleGroupExt) group).getLastNonNull();
			if (index < startIndex) {
				return null;
			}
			var value = ((SimpleGroupExt) group).getObject(index, row);
			var type = group.getType().getType(index);
			if (value instanceof Binary) {
				var annotation = type.getLogicalTypeAnnotation();
				if (annotation != null && "STRING".equals(annotation.toString())) {
					return new String(((Binary) value).getBytes(), StandardCharsets.UTF_8);
				}
			}
			return value;
		}
	}

	static class InputFileInfo {
		final Path path;
		final HadoopInputFile file;
		final ParquetMetadata metadata;
		final MessageColumnIO columnIO;

		InputFileInfo(Path path, HadoopInputFile file, ParquetMetadata metadata) {
			this.path = path;
			this.file = file;
			this.metadata = metadata;
			this.columnIO = new ColumnIOFactory().getColumnIO(metadata.getFileMetaData().getSchema());
		}
	}

	static class WriterState {
		java.nio.file.Path file;
		ParquetWriter<KvinRecord> writer;
		int year;
		int week;
		long[] minMax = {Long.MAX_VALUE, Long.MIN_VALUE};

		WriterState(java.nio.file.Path file, ParquetWriter<KvinRecord> writer, int year, int week) {
			this.file = file;
			this.writer = writer;
			this.year = year;
			this.week = week;
		}
	}

	static class IdMappings {
		long itemId, propertyId, contextId;
	}

	static class WriteContext {
		boolean hasExistingData;
		long itemIdCounter = 0, propertyIdCounter = 0, contextIdCounter = 0;
		long lastItemId = 0;
		Map<String, Long> itemMap = new HashMap<>();
		Map<String, Long> propertyMap = new HashMap<>();
		Map<String, Long> contextMap = new HashMap<>();
	}
}
