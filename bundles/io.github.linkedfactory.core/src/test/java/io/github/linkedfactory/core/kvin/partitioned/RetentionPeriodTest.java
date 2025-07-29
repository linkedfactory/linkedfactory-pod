package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;

public class RetentionPeriodTest {
	static String itemTemplate = "http://localhost:8080/linkedfactory/demofactory/{}";
	static String propertyTemplate = "http://example.org/{}";
	KvinTupleGenerator tupleGenerator;
	KvinPartitioned kvinPartitioned;
	File tempDir;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("kvinPartitioned").toFile();
		tupleGenerator = new KvinTupleGenerator()
				.setItems(10)
				.setPropertiesPerItem(10)
				.setValuesPerProperty(10)
				.setItemPattern(itemTemplate)
				.setPropertyPattern(propertyTemplate);
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(tempDir);
	}

	@Test
	public void testRetentionPeriod() throws IOException {
		URI item3 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 3);
		URI property = URIs.createURI("http://example.org/" + 1);

		long now = System.currentTimeMillis();
		long weekDuration = Duration.of(7, ChronoUnit.DAYS).toMillis();

		kvinPartitioned = new KvinPartitioned(tempDir, null, null);

		kvinPartitioned.put(tupleGenerator.setStartTime(now - weekDuration * 4).generate());
		kvinPartitioned.put(tupleGenerator.setStartTime(now - weekDuration * 2).generate());
		kvinPartitioned.put(tupleGenerator.setStartTime(now).generate());

		kvinPartitioned.runArchival();

		assertEquals(2, kvinPartitioned.fetch(item3, property, Kvin.DEFAULT_CONTEXT,
				Long.MAX_VALUE, now, 2, 0, null).toList().size());
		assertEquals(2, kvinPartitioned.fetch(item3, property, Kvin.DEFAULT_CONTEXT,
				now - weekDuration * 2, 0, 2, 0, null).toList().size());

		kvinPartitioned.close();

		kvinPartitioned = new KvinPartitioned(tempDir, null, Duration.of(7, ChronoUnit.DAYS));
		kvinPartitioned.runArchival();

		assertEquals(2, kvinPartitioned.fetch(item3, property, Kvin.DEFAULT_CONTEXT,
				 Long.MAX_VALUE, now, 2, 0, null).toList().size());
		assertEquals(0, kvinPartitioned.fetch(item3, property, Kvin.DEFAULT_CONTEXT,
				now - weekDuration * 2, 0, 2, 0, null).toList().size());

		kvinPartitioned.close();
	}

}
