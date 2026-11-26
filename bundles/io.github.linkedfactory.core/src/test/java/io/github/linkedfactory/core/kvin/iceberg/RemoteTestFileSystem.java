package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Hadoop file system for the scheme {@code kvintest} that stores files in the local file
 * system. Archive locations with this scheme are remote for the store, so tests use it to
 * cover the code paths for remote file systems such as S3 without a remote service.
 */
public class RemoteTestFileSystem extends RawLocalFileSystem {
	public static final String SCHEME = "kvintest";
	/**
	 * A Hadoop property whose values are recorded in {@link #MARKERS} when an instance is
	 * initialized, to check which configuration a store uses.
	 */
	public static final String MARKER = "kvintest.marker";
	public static final Set<String> MARKERS = ConcurrentHashMap.newKeySet();

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		String marker = conf.get(MARKER);
		if (marker != null) {
			MARKERS.add(marker);
		}
	}

	@Override
	public String getScheme() {
		return SCHEME;
	}

	@Override
	public URI getUri() {
		return URI.create(SCHEME + ":///");
	}
}
