package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * The settings of an Iceberg archive, for {@link KvinIceberg}, {@link IcebergRecordStore}
 * and {@link IcebergIdStore}.
 *
 * <p>All settings except the partitioning factory use JDK types only, so that callers such as
 * the OSGi service configuration need no Iceberg or Hadoop classes.</p>
 */
public final class KvinIcebergOptions {
	private final String location;
	private final Duration retentionPeriod;
	private final Duration cleanUpInterval;
	private final Function<Schema, PartitionSpec> partitioning;
	private final Map<String, String> tableProperties;
	private final Map<String, String> hadoopProperties;

	private KvinIcebergOptions(Builder builder) {
		location = builder.location;
		retentionPeriod = builder.retentionPeriod;
		cleanUpInterval = builder.cleanUpInterval;
		partitioning = builder.partitioning;
		tableProperties = Map.copyOf(builder.tableProperties);
		hadoopProperties = Map.copyOf(builder.hadoopProperties);
	}

	/**
	 * Starts the settings of an archive at a location, see {@link Builder#location(String)}.
	 */
	public static Builder builder(String location) {
		return new Builder().location(location);
	}

	/**
	 * Returns whether a location has a URI scheme, such as {@code s3a:} or {@code file:},
	 * rather than being a plain path. Single-letter schemes are Windows drive letters.
	 */
	public static boolean hasUriScheme(String location) {
		return location.startsWith("file:") || ArchiveLocation.localPath(location) == null;
	}

	public String location() {
		return location;
	}

	public Duration retentionPeriod() {
		return retentionPeriod;
	}

	public Duration cleanUpInterval() {
		return cleanUpInterval;
	}

	public Function<Schema, PartitionSpec> partitioning() {
		return partitioning;
	}

	public Map<String, String> tableProperties() {
		return tableProperties;
	}

	public Map<String, String> hadoopProperties() {
		return hadoopProperties;
	}

	/**
	 * Creates a Hadoop configuration with the default resources and the Hadoop properties.
	 * Creating one parses the default resources, so stores create it once.
	 */
	Configuration newHadoopConfiguration() {
		Configuration conf = new Configuration();
		hadoopProperties.forEach(conf::set);
		return conf;
	}

	public Builder toBuilder() {
		Builder builder = new Builder();
		builder.location = location;
		builder.retentionPeriod = retentionPeriod;
		builder.cleanUpInterval = cleanUpInterval;
		builder.partitioning = partitioning;
		builder.tableProperties.putAll(tableProperties);
		builder.hadoopProperties.putAll(hadoopProperties);
		return builder;
	}

	public static final class Builder {
		private String location;
		private Duration retentionPeriod;
		private Duration cleanUpInterval;
		private Function<Schema, PartitionSpec> partitioning;
		private final Map<String, String> tableProperties = new LinkedHashMap<>();
		private final Map<String, String> hadoopProperties = new LinkedHashMap<>();

		private Builder() {
		}

		/**
		 * The archive root: a local path, a {@code file:} URI, or a URI of another Hadoop
		 * file system, such as {@code s3a://bucket/archive}.
		 */
		public Builder location(String location) {
			this.location = Objects.requireNonNull(location, "location");
			return this;
		}

		/**
		 * With a retention period, a cleanup removes the values that are older. It must be
		 * positive; {@code null} keeps all values.
		 */
		public Builder retentionPeriod(Duration retentionPeriod) {
			if (retentionPeriod != null && (retentionPeriod.isNegative() || retentionPeriod.isZero())) {
				throw new IllegalArgumentException("Retention period must be positive: " + retentionPeriod);
			}
			this.retentionPeriod = retentionPeriod;
			return this;
		}

		/**
		 * Time between two cleanups that {@link KvinIceberg} runs in a background thread,
		 * starting one interval after it opens. {@code null} runs no cleanups. Only a
		 * {@link KvinIceberg} that opens the archive itself runs them.
		 */
		public Builder cleanUpInterval(Duration cleanUpInterval) {
			if (cleanUpInterval != null && (cleanUpInterval.isNegative() || cleanUpInterval.isZero())) {
				throw new IllegalArgumentException("Cleanup interval must be positive: " + cleanUpInterval);
			}
			this.cleanUpInterval = cleanUpInterval;
			return this;
		}

		/**
		 * Configures partitioning for a new value table, or verifies the spec of an existing
		 * one. {@code null} adopts the existing spec and creates new tables unpartitioned.
		 */
		public Builder partitioning(Function<Schema, PartitionSpec> partitioning) {
			this.partitioning = partitioning;
			return this;
		}

		/**
		 * Like {@link #partitioning(Function)}, with a partition spec in Iceberg's JSON format.
		 * The JSON is parsed when the store opens.
		 */
		public Builder partitionSpec(String json) {
			return partitioning(json == null ? null : schema -> PartitionSpecParser.fromJson(schema, json));
		}

		/**
		 * Iceberg table properties of the value table. They are set when the table is created,
		 * and changed values are committed when it is opened again. Properties that are left
		 * out keep their values in the table.
		 */
		public Builder tableProperties(Map<String, String> properties) {
			tableProperties.putAll(properties);
			return this;
		}

		public Builder tableProperty(String key, String value) {
			tableProperties.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
			return this;
		}

		/**
		 * Hadoop configuration properties for accessing the archive, for example
		 * {@code fs.s3a.endpoint}. They apply to the value table and the URI-ID tables.
		 */
		public Builder hadoopProperties(Map<String, String> properties) {
			hadoopProperties.putAll(properties);
			return this;
		}

		public Builder hadoopProperty(String key, String value) {
			hadoopProperties.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
			return this;
		}

		public KvinIcebergOptions build() {
			Objects.requireNonNull(location, "location");
			return new KvinIcebergOptions(this);
		}
	}
}
