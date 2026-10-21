package io.github.linkedfactory.core.rdf4j.fts;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LanceFtsIndexRegistry {
	private static final Map<String, LanceFtsIndex> INDEXES = new ConcurrentHashMap<>();

	private LanceFtsIndexRegistry() {
	}

	public static LanceFtsIndex getOrCreate(String indexDir) {
		String key = Path.of(indexDir).toAbsolutePath().normalize().toString();
		LanceFtsIndex existing = INDEXES.get(key);
		if (existing != null) {
			return existing;
		}
		synchronized (INDEXES) {
			existing = INDEXES.get(key);
			if (existing != null) {
				return existing;
			}
			LanceFtsIndex created = new LanceFtsIndex(key);
			INDEXES.put(key, created);
			return created;
		}
	}
}
