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
		config.setEndpoint("http://localhost:9200");
		config.setBulkPath("/api/bulk");
		config.setFailOnError(false);

		Model model = new LinkedHashModel();
		var implNode = config.export(model);

		FtsSailConfig parsed = new FtsSailConfig();
		parsed.parse(model, implNode);

		assertEquals("http://localhost:9200", parsed.getEndpoint());
		assertEquals("/api/bulk", parsed.getBulkPath());
		assertFalse(parsed.isFailOnError());
	}
}
