package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KvinMemTableTest {
	private final URI item = URIs.createURI("http://example.org/item");
	private final URI other = URIs.createURI("http://example.org/other");
	private final URI property = URIs.createURI("http://example.org/property");
	private final URI contextA = URIs.createURI("ctx:a");
	private final URI contextB = URIs.createURI("ctx:b");

	private Path directory;
	private KvinMemTable store;

	@Before
	public void setup() throws IOException {
		directory = Files.createTempDirectory("memtable-test");
		store = new KvinMemTable(directory);
	}

	@After
	public void cleanup() throws IOException {
		store.close();
		FileUtils.deleteDirectory(directory.toFile());
	}

	private long id(URI uri, KvinMemTable.EntryType type) {
		return store.toId(uri, type, false, null);
	}

	private int count(URI item, URI context) {
		return store.tableFiles.fetch(id(item, KvinMemTable.EntryType.SubjectToId),
				id(property, KvinMemTable.EntryType.PropertyToId),
				id(context, KvinMemTable.EntryType.ContextToId), 100).toList().size();
	}

	@Test
	public void deletesContext() {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextA, item, property, 2, 0, 2),
				new KvinTuple(contextA, other, property, 1, 0, 3),
				new KvinTuple(contextB, item, property, 1, 0, 4),
				new KvinTuple(Kvin.DEFAULT_CONTEXT, item, property, 1, 0, 5));

		assertEquals(3, store.delete(contextA));
		assertEquals(0, count(item, contextA));
		assertEquals(0, count(other, contextA));
		assertEquals(1, count(item, contextB));
		assertEquals(1, count(item, Kvin.DEFAULT_CONTEXT));

		assertEquals(0, store.delete(contextA));
		assertEquals(0, store.delete(URIs.createURI("ctx:unknown")));

		store.put(new KvinTuple(contextA, other, property, 3, 0, 6));
		assertEquals(1, count(other, contextA));

		// null selects the default context
		assertEquals(1, store.delete((URI) null));
		assertEquals(0, count(item, Kvin.DEFAULT_CONTEXT));
		assertEquals(List.of(), store.tableFiles.properties(id(item, KvinMemTable.EntryType.SubjectToId),
				id(Kvin.DEFAULT_CONTEXT, KvinMemTable.EntryType.ContextToId)).toList());
	}

	@Test
	public void deletesNothingBeforeTheFirstWrite() {
		assertEquals(0, store.delete(contextA));
	}

	@Test
	public void keepsContextDeleteAfterReopening() throws IOException {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextB, item, property, 1, 0, 2));
		assertEquals(1, store.delete(contextA));
		store.close();

		store = new KvinMemTable(directory);
		store.put(new KvinTuple(contextB, other, property, 2, 0, 3));
		assertEquals(0, count(item, contextA));
		assertEquals(1, count(item, contextB));
		assertEquals(1, count(other, contextB));
	}
}
