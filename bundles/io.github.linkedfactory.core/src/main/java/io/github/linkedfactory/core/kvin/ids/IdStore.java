package io.github.linkedfactory.core.kvin.ids;

import net.enilink.komma.core.URI;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * A store for mapping between URIs and IDs.
 */
public interface IdStore {
	/**
	 * Returns the ID for a given URI and kind. If the URI does not exist in the
	 * store, a new ID is created if <code>createIfNotExists</code> is set to
	 * <code>true</code>.
	 *
	 * @param uri               The URI.
	 * @param kind              The kind of the ID.
	 * @param createIfNotExists Whether a new ID should be created if the URI does
	 *                          not exist in the store.
	 * @return The ID for the given URI and kind.
	 */
	long getId(URI uri, IdKind kind, boolean createIfNotExists);

	/**
	 * Resolves distinct URIs of one kind. Unknown URIs are omitted when creation
	 * is disabled. Result order is unspecified; duplicate inputs share one mapping.
	 * Implementations should override this method to perform bulk storage reads.
	 */
	default Map<URI, Long> getIds(Collection<URI> uris, IdKind kind, boolean createIfNotExists) {
		Map<URI, Long> result = new HashMap<>();
		for (URI uri : new LinkedHashSet<>(uris)) {
			long id = getId(uri, kind, createIfNotExists);
			if (id != 0) {
				result.put(uri, id);
			}
		}
		return result;
	}

	/**
	 * Returns the URI for a given ID and kind.
	 *
	 * @param id   The ID.
	 * @param kind The kind of the ID.
	 * @return The URI for the given ID and kind.
	 */
	URI getUri(long id, IdKind kind);

	/**
	 * Resolves distinct IDs of one kind. Result order is unspecified.
	 * Implementations should override this method to perform bulk storage reads.
	 *
	 * @throws IllegalStateException if an ID has no URI mapping
	 */
	default Map<Long, URI> getUris(Collection<Long> ids, IdKind kind) {
		Map<Long, URI> result = new HashMap<>();
		for (long id : new HashSet<>(ids)) {
			URI uri = getUri(id, kind);
			if (uri == null) {
				throw new IllegalStateException("Unknown URI ID: " + id);
			}
			result.put(id, uri);
		}
		return result;
	}

	/**
	 * Flushes any pending changes to the store.
	 */
	void flush();
}
