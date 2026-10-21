package io.github.linkedfactory.core.rdf4j.fts.config;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class FtsSailConfigTest {
	@Test
	public void exportAndParseRoundtripKeepsValues() {
		FtsSailConfig config = new FtsSailConfig();
		config.setBackend("elastic");
		config.setEndpoint("http://localhost:9200");
		config.setBulkPath("/api/bulk");
		config.setSearchPath("/api/search");
		config.setExcludedModels(List.of("urn:model:A", "urn:model:B"));
		config.setFailOnError(false);
		config.setOutboxDir("/var/lib/fts-outbox");
		config.setIndexDir("/var/lib/fts-index");
		config.setDefaultLimit(25);
		config.setRequestTimeoutMillis(120_000);

		Model model = new LinkedHashModel();
		var implNode = config.export(model);

		FtsSailConfig parsed = new FtsSailConfig();
		parsed.parse(model, implNode);

		assertEquals("elastic", parsed.getBackend());
		assertEquals("http://localhost:9200", parsed.getEndpoint());
		assertEquals("/api/bulk", parsed.getBulkPath());
		assertEquals("/api/search", parsed.getSearchPath());
		assertArrayEquals(List.of("urn:model:A", "urn:model:B").toArray(), parsed.getExcludedModels().toArray());
		assertFalse(parsed.isFailOnError());
		assertEquals("/var/lib/fts-outbox", parsed.getOutboxDir());
		assertEquals("/var/lib/fts-index", parsed.getIndexDir());
		assertEquals(25, parsed.getDefaultLimit());
		assertEquals(120_000, parsed.getRequestTimeoutMillis());
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsUnlimitedRequestTimeout() {
		new FtsSailConfig().setRequestTimeoutMillis(0);
	}

	@Test(expected = org.eclipse.rdf4j.sail.config.SailConfigException.class)
	public void rejectsInvalidParsedRequestTimeout() {
		FtsSailConfig config = new FtsSailConfig();
		Model model = new LinkedHashModel();
		var implNode = config.export(model);
		var vf = org.eclipse.rdf4j.model.impl.SimpleValueFactory.getInstance();
		var timeout = vf.createIRI(FtsSailConfig.CONFIG_NS, "requestTimeoutMillis");
		model.remove(implNode, timeout, null);
		model.add(implNode, timeout, vf.createLiteral(-1));
		config.parse(model, implNode);
	}
}
