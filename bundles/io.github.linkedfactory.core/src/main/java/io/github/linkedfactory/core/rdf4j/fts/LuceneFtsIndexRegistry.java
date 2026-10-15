package io.github.linkedfactory.core.rdf4j.fts;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LuceneFtsIndexRegistry {
	private static final Map<String, LuceneFtsIndex> INDEXES = new ConcurrentHashMap<>();

	private LuceneFtsIndexRegistry() {
	}

	public static LuceneFtsIndex getOrCreate(String indexDir) throws IOException {
		String key = Path.of(indexDir).toAbsolutePath().normalize().toString();
		LuceneFtsIndex existing = INDEXES.get(key);
		if (existing != null) {
			return existing;
		}
		synchronized (INDEXES) {
			existing = INDEXES.get(key);
			if (existing != null) {
				return existing;
			}
			LuceneFtsIndex created = new LuceneFtsIndex(Path.of(key));
			INDEXES.put(key, created);
			return created;
		}
	}
}
