package io.github.linkedfactory.core.kvin.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.util.AggregatingIterator;
import io.github.linkedfactory.core.kvin.util.Values;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.*;

public class KvinScyllaDb implements Kvin {
	CqlSession session;
	String keyspace;
	List<KvinListener> listeners = new ArrayList<>();
	PreparedStatement dataInsertStmt, metaDataInsertStmt;

	public KvinScyllaDb(String keyspace) {
		this.keyspace = keyspace;
		initializeDatabase();
	}

	@Override
	public boolean addListener(KvinListener listener) {
		listeners.add(listener);
		return true;
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		listeners.remove(listener);
		return true;
	}

	private void initializeDatabase() {
		// will try to connect to the ScyllaDB server with default host (127.0.0.1) and port (9042)
		session = CqlSession.builder().build();
		// keyspace creation (with development config)
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
				+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
		//kvinData table creation
		session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".kvinData (" +
				"item text," +
				"timeRange text," +
				"property text," +
				"context text," +
				"time bigint," +
				"seqNr int," +
				"value blob," +
				"PRIMARY KEY((item, timeRange), context, property, time, seqNr));");
		//metadata table creation
		session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".kvinMetadata (" +
				"item text," +
				"property text," +
				"context text," +
				"timeRange text," +
				"PRIMARY KEY(item, context, property));");

		// put prepared statements
		dataInsertStmt = session.prepare("INSERT INTO " + keyspace + ".kvinData(" +
				"item, timeRange, property, context, time, seqNr, value) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?);");
		metaDataInsertStmt = session.prepare("INSERT INTO " + keyspace + ".kvinMetadata(" +
				"item, property, context, timeRange) " +
				"VALUES (?, ?, ?, ?) IF NOT EXISTS;");
	}

	@Override
	public void put(KvinTuple... tuples) {
		put(Arrays.asList(tuples));
	}

	@Override
	public void put(Iterable<KvinTuple> tuples) {
		try {
			putInternal(tuples);
		} catch (IOException | SQLException jsonProcessingException) {
			throw new RuntimeException(jsonProcessingException);
		}
	}

	private void putInternal(Iterable<KvinTuple> tuples) throws IOException, SQLException {
		URI previousItem = null;

		Set<String> currentItemProperties = new HashSet<>();

		BatchStatementBuilder dataBatch = BatchStatement.builder(DefaultBatchType.LOGGED);
		BatchStatementBuilder metadataBatch = BatchStatement.builder(DefaultBatchType.LOGGED);

		Iterator<KvinTuple> iterator = tuples.iterator();
		boolean isEndOfAllTuples;
		while (iterator.hasNext()) {
			KvinTuple tuple = iterator.next();
			isEndOfAllTuples = !iterator.hasNext();

			// building kvinData batch
			YearMonth tupleYearMonth = getYearMonth(tuple.time);
			String timeRange = tupleYearMonth.getMonthValue() + "" + tupleYearMonth.getYear();
			dataBatch.addStatement(dataInsertStmt.bind(
					tuple.item.toString(),
					timeRange,
					tuple.property.toString(),
					tuple.context.toString(),
					tuple.time,
					tuple.seqNr,
					ByteBuffer.wrap(encodeTupleValue(tuple.value))));

			// inserting metadata on item change
			if (isEndOfAllTuples || previousItem != null && !tuple.item.equals(previousItem)) {
				// batch insert into kvinData table
				session.execute(dataBatch.build());
				dataBatch.clearStatements();

				if (isEndOfAllTuples) {
					currentItemProperties.add(tuple.property.toString());
				}

				for (String itemProperty : currentItemProperties) {
					// building timeRange kvinMetadata batch
					YearMonth propertyYearMonth = getYearMonth(tuple.time);
					metadataBatch.addStatement(metaDataInsertStmt.bind(
							previousItem.toString(),
							itemProperty,
							tuple.context.toString(),
							propertyYearMonth.getMonthValue() + "" + propertyYearMonth.getYear()
					));
				}
				// inserting metadata batch
				session.execute(metadataBatch.build());
				metadataBatch.clearStatements();

				currentItemProperties.clear();
			}

			previousItem = tuple.item;
			currentItemProperties.add(tuple.property.toString());
		}
	}

	private YearMonth getYearMonth(long timestamp) {
		return YearMonth.from(Instant.ofEpochSecond(timestamp).atZone(ZoneId.systemDefault()));
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return fetchInternal(item, property, context, Long.MAX_VALUE, 0L, limit);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit,
	                                          long interval, String op) {
		IExtendedIterator<KvinTuple> internalResult = fetchInternal(item, property, context, end, begin, op == null ? limit : 0L);
		if (op != null) {
			internalResult = new AggregatingIterator<>(internalResult, interval, op.trim().toLowerCase(), limit) {
				@Override
				protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
					return new KvinTuple(item, property, context, time, seqNr, value);
				}
			};
		}
		return internalResult;
	}

	private IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, long end, long begin,
	                                                   long limit) {
		if (item == null) return NiceIterator.emptyIterator();

		if (limit > 0 || begin > 0) {
			return getLimitIterator(item, property, context, end, begin, limit);
		} else {
			return getSimpleIterator(item, property, context);
		}
	}

	private IExtendedIterator<KvinTuple> getLimitIterator(URI item, URI property, URI context, long end, long begin,
	                                                      long limit) {
		return new NiceIterator<>() {

			Iterator<Row> itemPropertiesIterator;
			Iterator<Row> itemIterator;
			Row currentEntry;
			PreparedStatement dataSelectStmt, propertiesSelectStmt;

			@Override
			public boolean hasNext() {
				if (itemPropertiesIterator == null && property == null) readPropertySet();
				if (itemIterator == null || (!itemIterator.hasNext() && property == null
						&& itemPropertiesIterator.hasNext())) {
					readNextProperty(property != null);
				}
				return itemIterator.hasNext();
			}

			@Override
			public KvinTuple next() {
				KvinTuple tuple = null;
				if (itemIterator.hasNext()) {
					currentEntry = itemIterator.next();
					URI item = URIs.createURI(Objects.requireNonNull(currentEntry.getString("item")));
					URI property = URIs.createURI(Objects.requireNonNull(currentEntry.getString("property")));
					URI context = URIs.createURI(Objects.requireNonNull(currentEntry.getString("context")));
					long time = currentEntry.getLong("time");
					int seqNr = currentEntry.getInt("seqNr");
					Object value;
					try {
						value = decodeTupleValue(currentEntry.getByteBuffer("value").array());
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
					tuple = new KvinTuple(item, property, context, time, seqNr, value);
					currentEntry = null;
				}
				return tuple;
			}

			private void readNextProperty(boolean isSingleItemPropertyRead) {
				// preparing statement
				if (itemIterator == null || !itemIterator.hasNext()) {
					if (dataSelectStmt == null) {
						String query = "SELECT * FROM " + keyspace
								+ ".kvinData WHERE item = ? AND timeRange IN(?) AND context = ? AND property = ? ";
						if (begin >= 0 && end <= Long.MAX_VALUE) query = query + "AND time >= ? AND time <= ? ";
						if (limit > 0) query = query + "limit ? ;";

						dataSelectStmt = session.prepare(query);
					}
					// binding
					String currentProperty = isSingleItemPropertyRead ? property.toString() :
							itemPropertiesIterator.next().getString("property");
					BoundStatement dataSelectBoundStmt;
					if (limit > 0) {
						dataSelectBoundStmt = dataSelectStmt.bind(item.toString(), getTimeRage(item,
										URIs.createURI(currentProperty), context), context.toString(), currentProperty,
								begin, end, limit).setPageSize(1000);
					} else {
						dataSelectBoundStmt = dataSelectStmt.bind(item.toString(), getTimeRage(item,
										URIs.createURI(currentProperty), context), context.toString(), currentProperty,
								begin, end).setPageSize(1000);
					}
					// execution
					itemIterator = session.execute(dataSelectBoundStmt).iterator();
				}
			}

			private void readPropertySet() {
				propertiesSelectStmt = session.prepare("SELECT property FROM " + keyspace
						+ ".kvinMetadata WHERE item = ? ;");
				BoundStatement propertiesSelectBoundStmt = propertiesSelectStmt.bind(item.toString()).setPageSize(1000);
				itemPropertiesIterator = session.execute(propertiesSelectBoundStmt).iterator();
			}

			@Override
			public void close() {
				super.close();
			}
		};
	}

	private IExtendedIterator<KvinTuple> getSimpleIterator(URI item, URI property, URI context) {
		return new NiceIterator<>() {
			Iterator<Row> entries;
			PreparedStatement dataSelectStmt;
			BoundStatement dataSelectBoundStmt;

			@Override
			public boolean hasNext() {
				if (entries == null) loadEntriesFromTable();
				return entries.hasNext();
			}

			@Override
			public KvinTuple next() {
				KvinTuple tuple = null;
				Row currentEntry = entries.next();
				if (currentEntry != null) {
					URI item = URIs.createURI(Objects.requireNonNull(currentEntry.getString("item")));
					URI property = URIs.createURI(Objects.requireNonNull(currentEntry.getString("property")));
					URI context = URIs.createURI(Objects.requireNonNull(currentEntry.getString("context")));
					long time = currentEntry.getLong("time");
					int seqNr = currentEntry.getInt("seqNr");
					Object value;
					try {
						value = decodeTupleValue(currentEntry.getByteBuffer("value").array());
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
					tuple = new KvinTuple(item, property, context, time, seqNr, value);
				}
				return tuple;
			}

			private void loadEntriesFromTable() {
				// preparing statement
				if (dataSelectStmt == null && property != null) {
					dataSelectStmt = session.prepare("SELECT * FROM " + keyspace + ".kvinData " +
							"WHERE item = ? AND timeRange IN(?) AND context = ? AND property = ? ;");
				} else if (dataSelectStmt == null && property == null) {
					dataSelectStmt = session.prepare("SELECT * FROM " + keyspace + ".kvinData " +
							"WHERE item = ? AND timeRange IN(?) AND context = ? ;");
				}

				//binding
				if (property != null) {
					dataSelectBoundStmt = dataSelectStmt.bind(item.toString(), getTimeRage(item, property, context),
							context.toString(), property.toString()).setPageSize(1000);
				} else if (property == null) {
					dataSelectBoundStmt = dataSelectStmt.bind(item.toString(), getTimeRage(item, null, context),
							context.toString()).setPageSize(1000);
				}
				// execution
				entries = session.execute(dataSelectBoundStmt).iterator();
			}

			@Override
			public void close() {
				super.close();
			}
		};
	}

	@Override
	public IExtendedIterator<URI> properties(URI item) {
		if (item == null) return NiceIterator.emptyIterator();
		return new NiceIterator<>() {
			Iterator<Row> propertyIterator;
			PreparedStatement propertiesSelectStmt;
			BoundStatement propertiesSelectBoundStmt;

			@Override
			public boolean hasNext() {
				if (propertyIterator == null) readPropertySet();
				return propertyIterator.hasNext();
			}

			@Override
			public URI next() {
				return URIs.createURI(propertyIterator.next().getString("property"));
			}

			private void readPropertySet() {
				propertiesSelectStmt = session.prepare("SELECT property FROM " + keyspace
						+ ".kvinMetadata WHERE item = ? ;");
				propertiesSelectBoundStmt = propertiesSelectStmt.bind(item.toString()).setPageSize(1000);
				propertyIterator = session.execute(propertiesSelectBoundStmt).iterator();
			}

			@Override
			public void close() {
				super.close();
			}
		};
	}

	private String getTimeRage(URI item, URI property, URI context) {
		PreparedStatement kvinMetadataSelectStatement;
		BoundStatement kvinMetadataBoundStatement;

		//preparing statements
		if (item != null && property == null) {
			kvinMetadataSelectStatement = session.prepare("SELECT timeRange FROM " + keyspace
					+ ".kvinMetadata WHERE item = ? AND context = ?;");
			kvinMetadataBoundStatement = kvinMetadataSelectStatement.bind(item.toString(), context.toString());
		} else if (item != null && property != null) {
			kvinMetadataSelectStatement = session.prepare("SELECT timeRange FROM " + keyspace
					+ ".kvinMetadata WHERE item = ? AND context = ? AND property = ?;");
			kvinMetadataBoundStatement = kvinMetadataSelectStatement.bind(item.toString(), context.toString(),
					property.toString());
		} else {
			throw new RuntimeException("Invalid parameters for timeRange lookup");
		}

		// converting query result to string for IN() operator value
		Iterator<Row> result = session.execute(kvinMetadataBoundStatement).iterator();
		Set<String> timeRangeSet = new HashSet<>();
		while (result.hasNext()) {
			timeRangeSet.add(String.valueOf(result.next().getString("timeRange")));
		}
		return String.join(", ", timeRangeSet);
	}

	private byte[] encodeTupleValue(Object record) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		if (record instanceof Record) {
			Record r = (Record) record;
			byteArrayOutputStream.write("O".getBytes(StandardCharsets.UTF_8));
			byte[] propertyBytes = r.getProperty().toString().getBytes();
			byteArrayOutputStream.write((byte) propertyBytes.length);
			byteArrayOutputStream.write(propertyBytes);
			byteArrayOutputStream.write(encodeTupleValue(r.getValue()));
		} else if (record instanceof URI) {
			URI uri = (URI) record;
			byte[] uriIndicatorBytes = "R".getBytes(StandardCharsets.UTF_8);
			byte[] uriBytes = new byte[uri.toString().getBytes().length + 1];
			uriBytes[0] = (byte) uri.toString().getBytes().length;
			System.arraycopy(uri.toString().getBytes(), 0, uriBytes, 1, uriBytes.length - 1);

			byte[] combinedBytes = new byte[uriIndicatorBytes.length + uriBytes.length];
			System.arraycopy(uriIndicatorBytes, 0, combinedBytes, 0, uriIndicatorBytes.length);
			System.arraycopy(uriBytes, 0, combinedBytes, uriIndicatorBytes.length, uriBytes.length);
			return combinedBytes;
		} else {
			return Values.encode(record);
		}
		return byteArrayOutputStream.toByteArray();
	}

	private Object decodeTupleValue(byte[] data) throws IOException {
		Record r = null;
		try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
			char type = (char) byteArrayInputStream.read();
			if (type == 'O') {
				int propertyLength = byteArrayInputStream.read();
				String property = new String(byteArrayInputStream.readNBytes(propertyLength), StandardCharsets.UTF_8);
				var value = decodeTupleValue(byteArrayInputStream.readAllBytes());
				if (r != null) {
					r.append(new Record(URIs.createURI(property), value));
				} else {
					r = new Record(URIs.createURI(property), value);
				}
			} else if (type == 'R') {
				int uriLength = byteArrayInputStream.read();
				String uri = new String(byteArrayInputStream.readNBytes(uriLength), StandardCharsets.UTF_8);
				return URIs.createURI(uri);
			} else {
				return Values.decode(data);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return r;
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		return 0;
	}

	@Override
	public boolean delete(URI item) {
		return false;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, long limit) {
		return null;
	}

	@Override
	public long approximateSize(URI item, URI property, URI context, long end, long begin) {
		return 0;
	}

	@Override
	public void close() {
		if (session != null) {
			session.close();
		}
	}
}
