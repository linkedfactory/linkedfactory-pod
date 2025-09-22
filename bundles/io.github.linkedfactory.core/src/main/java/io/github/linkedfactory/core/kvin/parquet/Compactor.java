package io.github.linkedfactory.core.kvin.parquet;

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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static io.github.linkedfactory.core.kvin.parquet.ParquetHelpers.*;

public class Compactor {
	final KvinParquet kvinParquet;
	final File compactionFolder;
	String archiveLocation;
	int dataFileCompactionTrigger, mappingFileCompactionTrigger;

	public Compactor(KvinParquet kvinParquet) {
		this(kvinParquet, 3, 3);
	}

	public Compactor(KvinParquet kvinParquet, int dataFileCompactionTrigger, int mappingFileCompactionTrigger) {
		this.archiveLocation = kvinParquet.archiveLocation;
		this.compactionFolder = new File(archiveLocation, ".compaction");
		this.kvinParquet = kvinParquet;
		this.dataFileCompactionTrigger = dataFileCompactionTrigger;
		this.mappingFileCompactionTrigger = mappingFileCompactionTrigger;
	}

	public void execute() throws IOException {
		// delete compaction folder if it exists
		FileUtils.deleteDirectory(compactionFolder);

		Set<String> compactedMappings;
		List<File> weekFolders;
		Lock readLock = kvinParquet.readLock();
		try {
			compactedMappings = compactMappingFiles();
			weekFolders = getCompactionEligibleWeekFolders();
			for (File weekFolder : weekFolders) {
				try {
					compactDataFiles(weekFolder);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		} finally {
			readLock.release();
		}

		if (!compactionFolder.exists()) {
			// nothing to do, compaction was not necessary
			return;
		}

		Lock writeLock = kvinParquet.writeLock();
		try {
			// replace existing files with compacted files
			if (!compactedMappings.isEmpty()) {
				// delete compacted mapping files
				deleteMappingFiles(Paths.get(archiveLocation, "metadata"), compactedMappings);
			}
			for (File weekFolder : weekFolders) {
				FileUtils.cleanDirectory(weekFolder);
			}
			java.nio.file.Path source = compactionFolder.toPath();
			java.nio.file.Path destination = Paths.get(archiveLocation);
			Files.walk(source)
					.skip(1)
					.filter(Files::isRegularFile)
					.forEach(p -> {
						java.nio.file.Path dest = destination.resolve(source.relativize(p));
						try {
							Files.createDirectories(dest.getParent());
							Files.move(p, dest);
						} catch (IOException e) {
							throw new UncheckedIOException(e);
						}
					});
			// completely delete compaction folder
			FileUtils.deleteDirectory(compactionFolder);
		} finally {
			// clear all caches
			kvinParquet.clearCaches();
			writeLock.release();
		}
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

	private Set<String> compactMappingFiles() throws IOException {
		Set<String> compacted = new HashSet<>();
		Map<String, List<Pair<String, Integer>>> mappingFiles = getMappingFiles(Paths.get(archiveLocation, "metadata"));
		for (Map.Entry<String, List<Pair<String, Integer>>> mapping : mappingFiles.entrySet()) {
			if (mapping.getValue().size() < mappingFileCompactionTrigger) {
				// do nothing if number of files for compaction is not yet reached
				continue;
			}
			compacted.add(mapping.getKey());

			Path compactedFile = new Path(new File(compactionFolder, "metadata").toString(), mapping.getKey() + "__1.parquet");
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
		}
		return compacted;
	}

	private ParquetReader<IdMapping> getParquetMappingReader(HadoopInputFile file) throws IOException {
		return AvroParquetReader.<IdMapping>builder(file)
				.withDataModel(reflectData)
				.useStatsFilter()
				.build();
	}

	private void compactDataFiles(File weekFolder) throws IOException {
		Lock readLock = kvinParquet.readLock();
		try {
			List<java.nio.file.Path> dataFiles = Files.walk(weekFolder.toPath(), 1)
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

			java.nio.file.Path targetFolder = compactionFolder.toPath().resolve(
					Paths.get(archiveLocation).relativize(weekFolder.toPath()));

			Path compactionFile = new Path(targetFolder.toAbsolutePath().toString(), "data__1.parquet");
			ParquetWriter<KvinRecord> compactionFileWriter = getKvinRecordWriter(compactionFile);

			PriorityQueue<Pair<KvinRecord, IExtendedIterator<KvinRecord>>> nextRecords =
					new PriorityQueue<>(Comparator.comparing(Pair::getFirst));
			for (java.nio.file.Path dataFile : dataFiles) {
				IExtendedIterator<KvinRecord> it = createKvinRecordReader(new Path(dataFile.toString()), null);
				if (it.hasNext()) {
					nextRecords.add(new Pair<>(it.next(), it));
				} else {
					it.close();
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
				} else {
					pair.getSecond().close();
				}
			}

			compactionFileWriter.close();
		} finally {
			readLock.release();
		}
	}
}
