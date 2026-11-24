package io.github.linkedfactory.core.rdf4j.fts;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class LuceneFtsIndexRegistry {
	private static final Map<String, Entry> ENTRIES = new HashMap<>();

	private LuceneFtsIndexRegistry() {
	}

	public static synchronized LuceneFtsIndex getOrCreate(String indexDir) throws IOException {
		String key = normalize(indexDir);
		Entry entry = ENTRIES.get(key);
		if (entry != null) {
			entry.refCount++;
			return entry.index;
		}
		LuceneFtsIndex created = new LuceneFtsIndex(Path.of(key));
		ENTRIES.put(key, new Entry(created));
		return created;
	}

	public static void release(String indexDir) throws IOException {
		LuceneFtsIndex toClose = null;
		synchronized (LuceneFtsIndexRegistry.class) {
			String key = normalize(indexDir);
			Entry entry = ENTRIES.get(key);
			if (entry == null) {
				return;
			}
			entry.refCount--;
			if (entry.refCount <= 0) {
				ENTRIES.remove(key);
				toClose = entry.index;
			}
		}
		if (toClose != null) {
			toClose.close();
		}
	}

	private static String normalize(String indexDir) {
		return Path.of(indexDir).toAbsolutePath().normalize().toString();
	}

	private static final class Entry {
		private final LuceneFtsIndex index;
		private int refCount = 1;

		private Entry(LuceneFtsIndex index) {
			this.index = index;
		}
	}
}
