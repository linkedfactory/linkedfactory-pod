package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.KvinException;
import io.github.linkedfactory.core.kvin.parquet.records.KvinRecord;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.util.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import static io.github.linkedfactory.core.kvin.parquet.ParquetHelpers.*;

public class Compactor {
	static final String COMPACTION_FOLDER = ".compaction";
	static final String JOURNAL = "journal";

	final KvinParquet kvinParquet;
	final File compactionFolder;
	String archiveLocation;
	int dataFileCompactionTrigger, mappingFileCompactionTrigger;

	public Compactor(KvinParquet kvinParquet) {
		this(kvinParquet, 3, 3);
	}

	public Compactor(KvinParquet kvinParquet, int dataFileCompactionTrigger, int mappingFileCompactionTrigger) {
		this.archiveLocation = kvinParquet.archiveLocation;
		this.compactionFolder = new File(archiveLocation, COMPACTION_FOLDER);
		this.kvinParquet = kvinParquet;
		this.dataFileCompactionTrigger = dataFileCompactionTrigger;
		this.mappingFileCompactionTrigger = mappingFileCompactionTrigger;
	}

	public void execute() throws IOException {
		if (compactionFolder.exists()) {
			// finish or discard an interrupted compaction
			Lock writeLock = kvinParquet.writeLock();
			try {
				recover(Paths.get(archiveLocation));
			} finally {
				kvinParquet.clearCaches();
				writeLock.release();
			}
		}

		// maps the path of each compacted file to the paths of the files it replaces
		Map<String, List<String>> replacements = new LinkedHashMap<>();
		Lock readLock = kvinParquet.readLock();
		try {
			compactMappingFiles(replacements);
			for (File weekFolder : getCompactionEligibleWeekFolders()) {
				compactDataFiles(weekFolder, replacements);
			}
		} finally {
			readLock.release();
		}

		if (replacements.isEmpty()) {
			// nothing to do, compaction was not necessary
			FileUtils.deleteDirectory(compactionFolder);
			return;
		}

		// from here on, the compaction is finished after a crash instead of being discarded
		writeJournal(replacements);

		Lock writeLock = kvinParquet.writeLock();
		try {
			replaceFiles(replacements);
			FileUtils.deleteDirectory(compactionFolder);
		} finally {
			// clear all caches
			kvinParquet.clearCaches();
			writeLock.release();
		}
	}

	void writeJournal(Map<String, List<String>> replacements) throws IOException {
		writeJournal(compactionFolder.toPath(), replacements);
	}

	void replaceFiles(Map<String, List<String>> replacements) throws IOException {
		replaceFiles(Paths.get(archiveLocation), compactionFolder.toPath(), replacements);
	}

	/**
	 * Finishes or discards a compaction that was interrupted, for example by a crash.
	 * <p>
	 * If the compaction has written its journal, all compacted files are complete and
	 * the replacement of the original files is finished. Otherwise, the original files
	 * have not been changed and the compacted files are discarded.
	 *
	 * @param archive the location of the archive
	 */
	static void recover(java.nio.file.Path archive) throws IOException {
		java.nio.file.Path folder = archive.resolve(COMPACTION_FOLDER);
		if (!Files.exists(folder)) {
			return;
		}
		java.nio.file.Path journal = folder.resolve(JOURNAL);
		if (Files.exists(journal)) {
			log.info("Finishing interrupted compaction of {}", archive);
			replaceFiles(archive, folder, readJournal(journal));
		}
		FileUtils.deleteDirectory(folder.toFile());
	}

	/**
	 * Writes the journal after syncing the compacted files to disk.
	 * The journal is created atomically, so it is either missing or complete.
	 */
	static void writeJournal(java.nio.file.Path folder, Map<String, List<String>> replacements) throws IOException {
		StringBuilder content = new StringBuilder();
		for (Map.Entry<String, List<String>> replacement : replacements.entrySet()) {
			sync(folder.resolve(replacement.getKey()));
			for (String original : replacement.getValue()) {
				content.append(replacement.getKey()).append('\t').append(original).append('\n');
			}
		}
		java.nio.file.Path tempJournal = folder.resolve(JOURNAL + ".tmp");
		try (FileChannel channel = FileChannel.open(tempJournal, StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
			channel.write(ByteBuffer.wrap(content.toString().getBytes(StandardCharsets.UTF_8)));
			channel.force(true);
		}
		Files.move(tempJournal, folder.resolve(JOURNAL), StandardCopyOption.ATOMIC_MOVE);
	}

	static Map<String, List<String>> readJournal(java.nio.file.Path journal) throws IOException {
		Map<String, List<String>> replacements = new LinkedHashMap<>();
		for (String line : Files.readAllLines(journal, StandardCharsets.UTF_8)) {
			if (line.isEmpty()) {
				continue;
			}
			String[] paths = line.split("\t", 2);
			if (paths.length != 2) {
				throw new KvinException("Invalid line in compaction journal " + journal + ": " + line);
			}
			replacements.computeIfAbsent(paths[0], k -> new ArrayList<>()).add(paths[1]);
		}
		return replacements;
	}

	/**
	 * Replaces the original files with the compacted files, one compacted file at a time.
	 * <p>
	 * A compacted file is moved to the archive only after all files it replaces are deleted.
	 * A compacted file that is still in the compaction folder therefore marks a replacement
	 * that is not finished, and calling this method again after a crash is safe.
	 */
	static void replaceFiles(java.nio.file.Path archive, java.nio.file.Path folder,
	                         Map<String, List<String>> replacements) throws IOException {
		for (Map.Entry<String, List<String>> replacement : replacements.entrySet()) {
			java.nio.file.Path compacted = folder.resolve(replacement.getKey());
			if (!Files.exists(compacted)) {
				// already replaced
				continue;
			}
			for (String original : replacement.getValue()) {
				java.nio.file.Path originalFile = archive.resolve(original);
				Files.deleteIfExists(originalFile);
				Files.deleteIfExists(checksumFile(originalFile));
			}
			java.nio.file.Path destination = archive.resolve(replacement.getKey());
			if (Files.exists(destination)) {
				throw new KvinException("Compacted file " + destination + " already exists");
			}
			Files.createDirectories(destination.getParent());
			java.nio.file.Path compactedChecksum = checksumFile(compacted);
			if (Files.exists(compactedChecksum)) {
				Files.move(compactedChecksum, checksumFile(destination), StandardCopyOption.REPLACE_EXISTING);
			}
			// moving the compacted file finishes its replacement
			Files.move(compacted, destination);
		}
	}

	/**
	 * Returns the checksum file that Hadoop writes next to a file.
	 */
	static java.nio.file.Path checksumFile(java.nio.file.Path file) {
		return file.resolveSibling("." + file.getFileName() + ".crc");
	}

	private static void sync(java.nio.file.Path file) throws IOException {
		try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
			channel.force(true);
		}
	}

	private String relativePath(java.nio.file.Path file) {
		return Paths.get(archiveLocation).relativize(file).toString().replace(File.separatorChar, '/');
	}

	private List<File> getCompactionEligibleWeekFolders() {
		List<File> weekFolderList = new ArrayList<>();
		File[] yearFolders = new File(archiveLocation).listFiles((file, s) ->
				!s.startsWith("meta") && !s.startsWith(".compaction"));
		for (File yearFolder : yearFolders) {
			File[] weekFolders = yearFolder.listFiles(File::isDirectory);
			for (File weekFolder : weekFolders) {
				File[] dataFiles = weekFolder.listFiles((file, s) -> s.endsWith(".parquet"));
				if (dataFiles.length >= dataFileCompactionTrigger) {
					weekFolderList.add(weekFolder);
				}
			}
		}
		return weekFolderList;
	}

	private void compactMappingFiles(Map<String, List<String>> replacements) throws IOException {
		Map<String, List<Pair<String, Integer>>> mappingFiles = getMappingFiles(Paths.get(archiveLocation, "metadata"));
		for (Map.Entry<String, List<Pair<String, Integer>>> mapping : mappingFiles.entrySet()) {
			if (mapping.getValue().size() < mappingFileCompactionTrigger) {
				// do nothing if number of files for compaction is not yet reached
				continue;
			}
			String compactedName = "metadata/" + mapping.getKey() + "__1.parquet";
			Path compactedFile = new Path(new File(compactionFolder, compactedName).toString());
			ParquetWriter<Object> compactedFileWriter = getParquetMappingWriter(compactedFile);

			PriorityQueue<Pair<IdMapping, ParquetReader<IdMapping>>> nextMappings =
					new PriorityQueue<>(Comparator.comparing(p -> p.getFirst().getValue()));
			for (Pair<String, Integer> file : mapping.getValue()) {
				ParquetReader<IdMapping> mappingFileReader = getParquetMappingReader(
						HadoopInputFile.fromPath(new Path(archiveLocation + "metadata/" + file.getFirst()), new Configuration()));
				IdMapping idMapping = mappingFileReader.read();
				if (idMapping != null) {
					nextMappings.add(new Pair<>(idMapping, mappingFileReader));
				} else {
					mappingFileReader.close();
				}
			}

			while (!nextMappings.isEmpty()) {
				var pair = nextMappings.poll();
				compactedFileWriter.write(pair.getFirst());

				IdMapping idMapping = pair.getSecond().read();
				if (idMapping != null) {
					nextMappings.add(new Pair<>(idMapping, pair.getSecond()));
				} else {
					pair.getSecond().close();
				}
			}
			compactedFileWriter.close();
			replacements.put(compactedName, mapping.getValue().stream()
					.map(file -> "metadata/" + file.getFirst()).toList());
		}
	}

	private ParquetReader<IdMapping> getParquetMappingReader(HadoopInputFile file) throws IOException {
		return AvroParquetReader.<IdMapping>builder(file)
				.withDataModel(reflectData)
				.useStatsFilter()
				.build();
	}

	/**
	 * Compacts the data files of a week folder into a single file in the compaction folder.
	 * <p>
	 * If a data file cannot be read, the week folder is not compacted. Its files are kept
	 * unchanged, as replacing them with a compacted file would lose the unreadable data.
	 *
	 * @param weekFolder   the week folder
	 * @param replacements receives the compacted file and the files it replaces
	 */
	private void compactDataFiles(File weekFolder, Map<String, List<String>> replacements) throws IOException {
		java.nio.file.Path targetFolder = compactionFolder.toPath().resolve(
				Paths.get(archiveLocation).relativize(weekFolder.toPath()));
		Lock readLock = kvinParquet.readLock();
		try {
			List<java.nio.file.Path> dataFiles;
			try (var paths = Files.walk(weekFolder.toPath(), 1)) {
				dataFiles = paths
						.skip(1)
						.filter(path -> path.getFileName().toString().startsWith("data_"))
						// sort descending by index
						.sorted(Comparator.comparing(p -> {
							try {
								return -Integer.parseInt(p.getFileName().toString()
										.replaceAll("^.*__", "")
										.replaceAll("\\..*$", ""));
							} catch (NumberFormatException nfe) {
								return 0;
							}
						}))
						.toList();
			}

			Path compactionFile = new Path(targetFolder.toAbsolutePath().toString(), "data__1.parquet");
			List<IExtendedIterator<KvinRecord>> readers = new ArrayList<>();
			try (ParquetWriter<KvinRecord> compactionFileWriter = getKvinRecordWriter(compactionFile)) {
				PriorityQueue<Pair<KvinRecord, IExtendedIterator<KvinRecord>>> nextRecords =
						new PriorityQueue<>(Comparator.comparing(Pair::getFirst));
				for (java.nio.file.Path dataFile : dataFiles) {
					IExtendedIterator<KvinRecord> it = createKvinRecordReader(new Path(dataFile.toString()), null);
					readers.add(it);
					if (it.hasNext()) {
						nextRecords.add(new Pair<>(it.next(), it));
					}
				}

				KvinRecord prevRecord = null;
				while (!nextRecords.isEmpty()) {
					var pair = nextRecords.poll();
					if (prevRecord == null || prevRecord.compareTo(pair.getFirst()) != 0) {
						var tuple = pair.getFirst();
						compactionFileWriter.write(tuple);
						prevRecord = tuple;
					}
					// else: omit tuple as it is duplicate in terms of id, time, and seqNr

					if (pair.getSecond().hasNext()) {
						nextRecords.add(new Pair<>(pair.getSecond().next(), pair.getSecond()));
					}
				}
			} catch (KvinException e) {
				log.error("Error while reading data files of {}. The folder is not compacted.", weekFolder, e);
				FileUtils.deleteDirectory(targetFolder.toFile());
				return;
			} finally {
				for (IExtendedIterator<KvinRecord> reader : readers) {
					reader.close();
				}
			}
			replacements.put(relativePath(weekFolder.toPath().resolve("data__1.parquet")),
					dataFiles.stream().map(this::relativePath).toList());
		} finally {
			readLock.release();
		}
	}
}
