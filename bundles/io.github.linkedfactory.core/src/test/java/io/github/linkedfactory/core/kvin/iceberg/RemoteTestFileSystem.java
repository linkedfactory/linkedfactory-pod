package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.hadoop.fs.RawLocalFileSystem;

import java.net.URI;

/**
 * A Hadoop file system for the scheme {@code kvintest} that stores files in the local file
 * system. Archive locations with this scheme are remote for the store, so tests use it to
 * cover the code paths for remote file systems such as S3 without a remote service.
 */
public class RemoteTestFileSystem extends RawLocalFileSystem {
	public static final String SCHEME = "kvintest";

	@Override
	public String getScheme() {
		return SCHEME;
	}

	@Override
	public URI getUri() {
		return URI.create(SCHEME + ":///");
	}
}
