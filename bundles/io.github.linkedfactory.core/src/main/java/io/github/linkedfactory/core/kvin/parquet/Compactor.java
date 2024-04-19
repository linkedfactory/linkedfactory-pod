package io.github.linkedfactory.core.kvin.parquet;

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
	String archiveLocation;
	KvinParquet kvinParquet;
	int dataFileCompactionTrigger = 2, mappingFileCompactionTrigger = 3;
	File compactionFolder;

	public Compactor(KvinParquet kvinParquet) {
		this.archiveLocation = kvinParquet.archiveLocation;
		this.compactionFolder = new File(archiveLocation, ".compaction");
		this.kvinParquet = kvinParquet;
	}

	public void execute() throws IOException {
		performMappingFileCompaction();
		List<File> weekFolders = getCompactionEligibleWeekFolders();
		for (File weekFolder : weekFolders) {
			try {
				compactDataFiles(weekFolder);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		if (!compactionFolder.exists()) {
			// nothing to do, compaction was not necessary
			return;
		}

		try {
			kvinParquet.writeLock.lock();
			clearCache();
			// replace existing files with compacted files
			FileUtils.cleanDirectory(new File(archiveLocation, "metadata"));
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
		} finally {
			kvinParquet.writeLock.unlock();
		}
	}

	private void performMappingFileCompaction() throws IOException {
		Map<String, List<Pair<String, Integer>>> mappingFiles = getMappingFiles(Paths.get(archiveLocation, "metadata"));
		if (mappingFiles.get("items").size() >= mappingFileCompactionTrigger) {
			try {
				kvinParquet.readLock.lock();
				generateCompactedMappingFiles(mappingFiles, new File(compactionFolder, "metadata"));
			} finally {
				kvinParquet.readLock.unlock();
			}
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

	private void generateCompactedMappingFiles(Map<String, List<Pair<String, Integer>>> mappingFiles,
	                                           File compactionFolder) throws IOException {
		for (Map.Entry<String, List<Pair<String, Integer>>> mapping : mappingFiles.entrySet()) {
			Path compactedFile = new Path(compactionFolder.toString(), mapping.getKey() + "__1.parquet");
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
				IdMapping idMapping = pair.getSecond().read();
				if (idMapping != null) {
					compactedFileWriter.write(idMapping);
					nextMappings.add(new Pair<>(idMapping, pair.getSecond()));
				} else {
					pair.getSecond().close();
				}
			}
			compactedFileWriter.close();
		}
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

			while (!nextTuples.isEmpty()) {
				var pair = nextTuples.poll();
				compactionFileWriter.write(pair.getFirst());
				KvinTupleInternal tuple = pair.getSecond().read();
				if (tuple != null) {
					nextTuples.add(new Pair<>(tuple, pair.getSecond()));
				} else {
					try {
						pair.getSecond().close();
					} catch (IOException e) {
					}
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
