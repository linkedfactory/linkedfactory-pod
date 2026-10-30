package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.ids.IdKind;
import io.github.linkedfactory.core.kvin.ids.IdStore;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.records.KvinRecordStore;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;

/**
 * URI-based KVIN facade composing a numeric record store and a URI-ID store.
 * The location constructors own their components; injected components remain
 * owned by the caller. ID mappings are flushed before records are committed.
 */
public class KvinIceberg implements Kvin {
	private static final int BATCH_SIZE = 8192;
	private static final int READ_BATCH_SIZE = 1024;
	private final KvinRecordStore recordStore;
	private final IdStore idStore;
	private final boolean owned;

	/**
	 * Opens an owned store using native Iceberg partition-spec JSON, or the
	 * default/adoption behavior when null. Keeps Iceberg types inside this bundle
	 * for callers such as the OSGi service configuration.
	 */
	public static KvinIceberg withPartitionSpec(String archiveLocation, String partitionSpec) {
		return new KvinIceberg(archiveLocation, null, partitionSpec == null ? null
				: schema -> PartitionSpecParser.fromJson(schema, partitionSpec));
	}

	public KvinIceberg(String archiveLocation) {
		this(archiveLocation, null);
	}

	public KvinIceberg(String archiveLocation, Duration retentionPeriod) {
		this(archiveLocation, retentionPeriod, null);
	}

	/**
	 * Creates an owned store with configurable record partitioning. A null factory
	 * adopts an existing table's spec, or creates an unpartitioned table.
	 */
	public KvinIceberg(String archiveLocation, Duration retentionPeriod,
			Function<Schema, PartitionSpec> partitioning) {
		if (retentionPeriod != null) {
			throw new UnsupportedOperationException("Iceberg retention cleanup is not implemented");
		}
		IcebergRecordStore records = new IcebergRecordStore(archiveLocation, retentionPeriod, partitioning);
		try {
			idStore = new IcebergIdStore(archiveLocation, records.hasSnapshot());
		} catch (RuntimeException e) {
			records.close();
			throw e;
		}
		recordStore = records;
		owned = true;
	}

	/**
	 * Composes caller-owned stores. Closing this facade does not close either store.
	 */
	public KvinIceberg(KvinRecordStore recordStore, IdStore idStore) {
		this.recordStore = Objects.requireNonNull(recordStore);
		this.idStore = Objects.requireNonNull(idStore);
		owned = false;
	}

	public KvinRecordStore getRecordStore() {
		return recordStore;
	}

	public IdStore getIdStore() {
		return idStore;
	}

	@Override
	public void put(KvinTuple... tuples) {
		put(Arrays.asList(tuples));
	}

	@Override
	public synchronized void put(Iterable<KvinTuple> tuples) {
		recordStore.put(() -> new Iterator<>() {
			final Iterator<KvinTuple> source = tuples.iterator();
			Iterator<KvinRecord> batch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				if (!batch.hasNext() && source.hasNext()) {
					List<KvinTuple> input = new ArrayList<>(BATCH_SIZE);
					while (input.size() < BATCH_SIZE && source.hasNext()) {
						input.add(source.next());
					}
					Map<URI, Long> items = idStore.getIds(input.stream().map(t -> t.item).distinct().toList(), IdKind.ITEM, true);
					Map<URI, Long> contexts = idStore.getIds(input.stream().map(t -> context(t.context)).distinct().toList(), IdKind.CONTEXT, true);
					Map<URI, Long> properties = idStore.getIds(input.stream().map(t -> t.property).distinct().toList(), IdKind.PROPERTY, true);
					idStore.flush();
					batch = input.stream().map(t -> new KvinRecord(items.get(t.item), contexts.get(context(t.context)),
							properties.get(t.property), t.time, t.seqNr, t.value)).iterator();
				}
				return batch.hasNext();
			}

			@Override
			public KvinRecord next() {
				if (!hasNext()) {
					throw new java.util.NoSuchElementException();
				}
				return batch.next();
			}
		});
	}

	private static URI context(URI context) {
		return context == null ? DEFAULT_CONTEXT : context;
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return fetchRows(List.of(item), property == null ? List.of() : List.of(property), context, null, null, limit, 0, null);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin,
			long limit, long interval, String op) {
		return fetch(List.of(item), property == null ? List.of() : List.of(property), context, end, begin, limit, interval, op);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(List<URI> items, List<URI> properties, URI context, long end,
			long begin, long limit, long interval, String op) {
		return fetchRows(items, properties, context, end, begin, limit, interval, op);
	}

	private synchronized IExtendedIterator<KvinTuple> fetchRows(List<URI> items, List<URI> properties, URI context,
			Long end, Long begin, long limit, long interval, String op) {
		if (items.isEmpty()) {
			return NiceIterator.emptyIterator();
		}
		Map<Long, URI> itemUris = reverse(idStore.getIds(items, IdKind.ITEM, false));
		URI requestedContext = context(context);
		long contextId = idStore.getId(requestedContext, IdKind.CONTEXT, false);
		if (itemUris.isEmpty() || contextId == 0) {
			return NiceIterator.emptyIterator();
		}
		Map<Long, URI> propertyUris = reverse(idStore.getIds(properties, IdKind.PROPERTY, false));
		if (!properties.isEmpty() && propertyUris.isEmpty()) {
			return NiceIterator.emptyIterator();
		}
		List<Long> itemIds = new ArrayList<>(itemUris.keySet());
		List<Long> propertyIds = new ArrayList<>(propertyUris.keySet());
		IExtendedIterator<KvinRecord> records = end == null
				? recordStore.fetch(itemIds.getFirst(), propertyIds.isEmpty() ? null : propertyIds.getFirst(), contextId, limit)
				: recordStore.fetch(itemIds, propertyIds, contextId, end, begin, limit, interval, op);
		return new NiceIterator<>() {
			final IExtendedIterator<KvinRecord> base = records;
			final int batchSize = properties.isEmpty()
					? (limit > 0 ? (int) Math.min(limit, READ_BATCH_SIZE) : READ_BATCH_SIZE) : 1;
			final Deque<KvinRecord> batch = new ArrayDeque<>();
			long lastItemId;
			long lastPropertyId;
			URI lastItemUri;
			URI lastPropertyUri;
			KvinTuple next;
			boolean closed;

			@Override
			public boolean hasNext() {
				if (next != null) {
					return true;
				}
				if (closed) {
					return false;
				}
				try {
					if (batch.isEmpty()) {
						Set<Long> missing = new HashSet<>();
						while (batch.size() < batchSize && base.hasNext()) {
							KvinRecord row = base.next();
							batch.addLast(row);
							if (!propertyUris.containsKey(row.propertyId())) {
								missing.add(row.propertyId());
							}
						}
						if (batch.isEmpty()) {
							close();
							return false;
						}
						if (!missing.isEmpty()) {
							propertyUris.putAll(idStore.getUris(missing, IdKind.PROPERTY));
						}
					}
					KvinRecord row = batch.removeFirst();
					if (lastPropertyUri == null || lastPropertyId != row.propertyId()) {
						lastPropertyUri = propertyUris.get(row.propertyId());
						if (lastPropertyUri == null) {
							throw new IllegalStateException("Unknown URI ID: " + row.propertyId());
						}
						lastPropertyId = row.propertyId();
					}
					if (lastItemUri == null || lastItemId != row.itemId()) {
						lastItemUri = itemUris.get(row.itemId());
						lastItemId = row.itemId();
					}
					next = new KvinTuple(lastItemUri, lastPropertyUri, requestedContext,
							row.time(), row.seqNr(), row.value());
					return true;
				} catch (RuntimeException e) {
					try {
						close();
					} catch (RuntimeException failure) {
						if (e != failure) {
							e.addSuppressed(failure);
						}
					}
					throw e;
				}
			}

			@Override
			public KvinTuple next() {
				if (!hasNext()) {
					throw new java.util.NoSuchElementException();
				}
				KvinTuple result = next;
				next = null;
				return result;
			}

			@Override
			public void close() {
				if (!closed) {
					closed = true;
					batch.clear();
					base.close();
				}
			}
		};
	}

	private static Map<Long, URI> reverse(Map<URI, Long> mappings) {
		Map<Long, URI> result = new HashMap<>();
		mappings.forEach((uri, id) -> result.put(id, uri));
		return result;
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		long itemId = idStore.getId(item, IdKind.ITEM, false);
		long contextId = idStore.getId(context(context), IdKind.CONTEXT, false);
		Long propertyId = property == null ? null : idStore.getId(property, IdKind.PROPERTY, false);
		if (itemId == 0 || contextId == 0 || (propertyId != null && propertyId == 0)) {
			return 0;
		}
		return recordStore.delete(itemId, propertyId, contextId, end, begin);
	}

	@Override
	public boolean delete(URI item, URI context) {
		long itemId = idStore.getId(item, IdKind.ITEM, false);
		long contextId = idStore.getId(context(context), IdKind.CONTEXT, false);
		return itemId != 0 && contextId != 0 && recordStore.delete(itemId, contextId);
	}

	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		long itemId = idStore.getId(item, IdKind.ITEM, false);
		long contextId = idStore.getId(context(context), IdKind.CONTEXT, false);
		if (itemId == 0 || contextId == 0) {
			return NiceIterator.emptyIterator();
		}
		try (IExtendedIterator<Long> properties = recordStore.properties(itemId, contextId)) {
			List<Long> ids = properties.toList();
			Map<Long, URI> uris = idStore.getUris(ids, IdKind.PROPERTY);
			return net.enilink.commons.iterator.WrappedIterator.create(ids.stream().map(uris::get).iterator());
		}
	}

	public void createBranch(String branchName) {
		if (recordStore instanceof IcebergRecordStore iceberg) {
			iceberg.createBranch(branchName);
		} else {
			throw new UnsupportedOperationException("Branches require an Iceberg record store");
		}
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		return NiceIterator.emptyIterator();
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		return NiceIterator.emptyIterator();
	}

	@Override
	public boolean addListener(KvinListener listener) {
		return false;
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		return false;
	}

	@Override
	public void close() {
		if (owned) {
			try {
				((IcebergIdStore) idStore).close();
			} finally {
				recordStore.close();
			}
		}
	}
}
