package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The location of an archive or of a file in it, as a string.
 *
 * <p>A location without a URI scheme, or with the scheme {@code file}, is a local path. Any
 * other scheme, for example {@code s3a://bucket/archive}, is a remote location that is
 * accessed through the Hadoop file system registered for the scheme. Single-letter schemes
 * are Windows drive letters and therefore local.</p>
 */
final class ArchiveLocation {
	// a URI scheme with at least two characters, followed by a colon
	private static final Pattern REMOTE_SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]+:");

	private final String location;
	private final Path localPath;

	private ArchiveLocation(String location, Path localPath) {
		this.location = location;
		this.localPath = localPath;
	}

	/**
	 * Parses a location. Local paths are made absolute and normalized, remote locations lose
	 * their trailing slashes.
	 */
	static ArchiveLocation of(String location) {
		Objects.requireNonNull(location, "location");
		Path local = localPath(location);
		if (local != null) {
			Path normalized = local.toAbsolutePath().normalize();
			return new ArchiveLocation(normalized.toString(), normalized);
		}
		String remote = location;
		while (remote.endsWith("/") && !remote.endsWith("://")) {
			remote = remote.substring(0, remote.length() - 1);
		}
		return new ArchiveLocation(remote, null);
	}

	/**
	 * Returns the local path of a location, or {@code null} if the location is remote.
	 */
	static Path localPath(String location) {
		if (location.startsWith("file:")) {
			return Path.of(java.net.URI.create(location));
		}
		if (REMOTE_SCHEME.matcher(location).find()) {
			return null;
		}
		return Path.of(location);
	}

	boolean isLocal() {
		return localPath != null;
	}

	/**
	 * The local path of this location, or {@code null} if it is remote.
	 */
	Path localPath() {
		return localPath;
	}

	/**
	 * Returns the location of a file or directory below this location.
	 */
	String resolve(String relative) {
		return localPath != null ? localPath.resolve(relative).toString() : location + "/" + relative;
	}

	/**
	 * Returns whether a file or directory below this location exists.
	 */
	boolean exists(String relative, Configuration conf) throws IOException {
		if (localPath != null) {
			return Files.exists(localPath.resolve(relative));
		}
		org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(resolve(relative));
		FileSystem fs = path.getFileSystem(conf);
		return fs.exists(path);
	}

	@Override
	public String toString() {
		return location;
	}
}
