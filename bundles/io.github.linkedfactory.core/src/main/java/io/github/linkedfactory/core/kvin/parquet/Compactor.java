package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.util.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;

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
	int dataFileCompactionTrigger = 2, mappingFileCompactionTrigger = 3;

	public Compactor(KvinParquet kvinParquet) {
		this.archiveLocation = kvinParquet.archiveLocation;
		this.compactionFolder = new File(archiveLocation, ".compaction");
		this.kvinParquet = kvinParquet;
	}

	public void execute() throws IOException {
		Set<String> compactedMappings;
		List<File> weekFolders;
		kvinParquet.readLock.lock();
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
			kvinParquet.readLock.unlock();
		}

		if (!compactionFolder.exists()) {
			// nothing to do, compaction was not necessary
			return;
		}

		try {
			kvinParquet.writeLock.lock();
			clearCache();
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
					.filter(p -> Files.isRegularFile(p))
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
			kvinParquet.writeLock.unlock();
		}
	}

	private List<File> getCompactionEligibleWeekFolders() {
		List<File> weekFolderList = new ArrayList<>();
		File[] yearFolders = new File(archiveLocation).listFiles((file, s) ->
				!s.startsWith("meta") && !s.startsWith(".compaction"));
		for (File yearFolder : yearFolders) {
			File[] weekFolders = yearFolder.listFiles((file) -> file.isDirectory());
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

	private ParquetReader<KvinTupleInternal> getParquetDataReader(HadoopInputFile file) throws IOException {
		return AvroParquetReader.<KvinTupleInternal>builder(file)
				.withDataModel(reflectData)
				.useStatsFilter()
				.build();
	}

	private void compactDataFiles(File weekFolder) throws IOException {
		try {
			kvinParquet.readLock.lock();
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
					.collect(Collectors.toList());

			java.nio.file.Path targetFolder = compactionFolder.toPath().resolve(
					Paths.get(archiveLocation).relativize(weekFolder.toPath()));

			Path compactionFile = new Path(targetFolder.toAbsolutePath().toString(), "data__1.parquet");
			ParquetWriter<KvinTupleInternal> compactionFileWriter = getParquetDataWriter(compactionFile);

			PriorityQueue<Pair<KvinTupleInternal, ParquetReader<KvinTupleInternal>>> nextTuples =
					new PriorityQueue<>(Comparator.comparing(Pair::getFirst));
			for (java.nio.file.Path dataFile : dataFiles) {
				ParquetReader<KvinTupleInternal> reader = getParquetDataReader(
						HadoopInputFile.fromPath(new Path(dataFile.toString()), new Configuration()));
				KvinTupleInternal tuple = reader.read();
				if (tuple != null) {
					nextTuples.add(new Pair<>(tuple, reader));
				} else {
					try {
						reader.close();
					} catch (IOException e) {
					}
				}
			}

			KvinTupleInternal prevTuple = null;
			while (!nextTuples.isEmpty()) {
				var pair = nextTuples.poll();
				if (prevTuple == null || prevTuple.compareTo(pair.getFirst()) != 0) {
					compactionFileWriter.write(pair.getFirst());
					prevTuple = pair.getFirst();
				} else if (prevTuple != null) {
					// omit tuple as it is duplicate in terms of id, time, and seqNr
				}

				KvinTupleInternal tuple = pair.getSecond().read();
				if (tuple != null) {
					nextTuples.add(new Pair<>(tuple, pair.getSecond()));
				} else {
					try {
						pair.getSecond().close();
					} catch (IOException e) {
					}
					break;
				}
			}

			compactionFileWriter.close();
		} finally {
			kvinParquet.readLock.unlock();
		}
	}

	private void clearCache() {
		synchronized (kvinParquet.inputFileCache) {
			kvinParquet.inputFileCache.clear();
		}
	}
}
