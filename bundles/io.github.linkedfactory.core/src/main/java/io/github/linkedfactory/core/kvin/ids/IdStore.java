package io.github.linkedfactory.core.kvin.ids;

import net.enilink.komma.core.URI;

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
	 * Returns the URI for a given ID and kind.
	 *
	 * @param id   The ID.
	 * @param kind The kind of the ID.
	 * @return The URI for the given ID and kind.
	 */
	URI getUri(long id, IdKind kind);
	
	/**
	 * Flushes any pending changes to the store.
	 */
	void flush();
}
