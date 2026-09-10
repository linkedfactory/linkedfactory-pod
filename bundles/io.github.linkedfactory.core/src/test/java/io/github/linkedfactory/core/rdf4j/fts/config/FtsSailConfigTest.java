package io.github.linkedfactory.core.rdf4j.fts.config;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FtsSailConfigTest {
	@Test
	public void exportAndParseRoundtripKeepsValues() throws Exception {
		FtsSailConfig config = new FtsSailConfig();
		config.setBackend("elastic");
		config.setEndpoint("http://localhost:9200");
		config.setBulkPath("/api/bulk");
		config.setSearchPath("/api/search");
		config.setFailOnError(false);
		config.setOutboxDir("/var/lib/fts-outbox");
		config.setDefaultLimit(25);

		Model model = new LinkedHashModel();
		var implNode = config.export(model);

		FtsSailConfig parsed = new FtsSailConfig();
		parsed.parse(model, implNode);

		assertEquals("elastic", parsed.getBackend());
		assertEquals("http://localhost:9200", parsed.getEndpoint());
		assertEquals("/api/bulk", parsed.getBulkPath());
		assertEquals("/api/search", parsed.getSearchPath());
		assertFalse(parsed.isFailOnError());
		assertEquals("/var/lib/fts-outbox", parsed.getOutboxDir());
		assertEquals(25, parsed.getDefaultLimit());
	}
}
