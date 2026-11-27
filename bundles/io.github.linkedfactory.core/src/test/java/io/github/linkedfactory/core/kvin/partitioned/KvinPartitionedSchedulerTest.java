package io.github.linkedfactory.core.kvin.partitioned;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the thread that starts the cyclic archivals.
 */
public class KvinPartitionedSchedulerTest {
	File tempDir;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("kvinPartitionedScheduler").toFile();
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(tempDir);
	}

	@Test
	public void cyclicArchivalDoesNotKeepJvmAlive() throws IOException {
		KvinPartitioned store = new KvinPartitioned(tempDir, Duration.ofHours(1), (Duration) null);
		try {
			List<Thread> schedulerThreads = Thread.getAllStackTraces().keySet().stream()
					.filter(thread -> thread.getName().equals("kvin-partitioned-scheduler"))
					.toList();
			assertFalse("The cyclic archival must run on the scheduler thread", schedulerThreads.isEmpty());
			for (Thread thread : schedulerThreads) {
				assertTrue("The scheduler thread must be a daemon thread", thread.isDaemon());
			}
		} finally {
			store.close();
		}
	}
}
